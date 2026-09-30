defmodule Horde.DynamicSupervisor.Member do
  @type t :: %Horde.DynamicSupervisor.Member{}
  @type status :: :uninitialized | :alive | :shutting_down | :dead
  defstruct [:status, :name, :pid]
end

defmodule Horde.DynamicSupervisorImpl do
  @moduledoc false

  require Logger
  use GenServer
  import Horde.TableUtils

  @recovery_retry_interval 1_000

  defstruct name: nil,
            members: %{},
            members_info: %{},
            processes_by_id: nil,
            process_pid_to_id: nil,
            local_process_count: 0,
            waiting_for_quorum: [],
            supervisor_ref_to_name: %{},
            name_to_supervisor_ref: %{},
            unavailable_members: %{},
            member_observations: %{},
            local_processes: %{},
            pending_recoveries: %{},
            recovery_retry_timer: nil,
            shutting_down: false,
            processes_supervisor_ready: false,
            supervisor_options: [],
            proxy_message_ttl: :infinity,
            proxy_operation_ttl: nil,
            distribution_strategy: Horde.UniformDistribution

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  ## GenServer callbacks
  defp crdt_name(name), do: :"#{name}.Crdt"
  defp supervisor_name(name), do: :"#{name}.ProcessesSupervisor"

  defp fully_qualified_name({name, node}) when is_atom(name) and is_atom(node), do: {name, node}
  defp fully_qualified_name(name) when is_atom(name), do: {name, node()}

  @doc false
  def init(options) do
    name = Keyword.get(options, :name)

    Logger.info("Starting #{inspect(__MODULE__)} with name #{inspect(name)}")

    Process.flag(:trap_exit, true)
    :ok = :net_kernel.monitor_nodes(true, node_type: :visible)

    state =
      %__MODULE__{
        supervisor_options: options,
        processes_by_id: new_table(:processes_by_id),
        process_pid_to_id: new_table(:process_pid_to_id),
        name: name
      }
      |> Map.merge(Map.new(Keyword.take(options, [:distribution_strategy, :proxy_message_ttl])))

    state = load_initial_state(state)

    {:ok, state, {:continue, {:set_members, Keyword.get(options, :members)}}}
  end

  def handle_continue({:set_members, nil}, state), do: {:noreply, state}

  def handle_continue({:set_members, :auto}, state) do
    state =
      state.name
      |> Horde.NodeListener.make_members()
      |> set_initial_members(state)

    {:noreply, state}
  end

  def handle_continue({:set_members, members}, state) do
    {:noreply, set_initial_members(members, state)}
  end

  defp set_initial_members(members, state) do
    # Peers can already have seeded the CRDT before this implementation starts.
    # Constructor membership is additive; explicit set_members remains replacing.
    set_members(
      Enum.uniq(Enum.map(members, &fully_qualified_name/1) ++ Map.keys(state.members)),
      state
    )
  end

  defp load_initial_state(state) do
    diffs =
      Enum.map(DeltaCrdt.to_map(crdt_name(state.name), :infinity), fn {key, value} ->
        {:add, key, value}
      end)

    state |> update_members(diffs) |> set_own_node_status() |> update_processes(diffs)
  end

  def on_diffs(name, diffs) do
    try do
      send(name, {:crdt_update, diffs})
    rescue
      ArgumentError ->
        # the process might already been stopped
        :ok
    end
  end

  defp node_info(state) do
    %Horde.DynamicSupervisor.Member{
      status: node_status(state),
      name: fully_qualified_name(state.name),
      pid: self()
    }
  end

  defp node_status(%{shutting_down: true}), do: :shutting_down
  defp node_status(%{processes_supervisor_ready: false}), do: :uninitialized
  defp node_status(_state), do: :alive

  @doc false
  def handle_call(:horde_shutting_down, _f, state) do
    state =
      %{state | shutting_down: true}
      |> set_own_node_status()

    {:reply, :ok, state}
  end

  def handle_call(:get_telemetry, _from, state) do
    telemetry = %{
      global_supervised_process_count: size_of(state.processes_by_id),
      local_supervised_process_count: state.local_process_count
    }

    {:reply, telemetry, state}
  end

  def handle_call(:local_processes, _from, state) do
    processes =
      Enum.map(state.local_processes, fn {id, {spec, pid}} -> {id, pid, spec} end)

    {:reply, processes, state}
  end

  def handle_call(:local_process_records, _from, state) do
    owner = fully_qualified_name(state.name)

    records =
      for {id, {^owner, spec, pid}} <- :ets.tab2list(state.processes_by_id), do: {id, pid, spec}

    {:reply, records, state}
  end

  def handle_call(:wait_for_quorum, from, state) do
    if state.distribution_strategy.has_quorum?(Map.values(members(state))) do
      {:reply, :ok, state}
    else
      {:noreply, %{state | waiting_for_quorum: [from | state.waiting_for_quorum]}}
    end
  end

  def handle_call({:set_members, members}, _from, state) do
    {:reply, :ok, set_members(members, state)}
  end

  def handle_call(:members, _from, state) do
    {:reply, Map.keys(state.members), state}
  end

  def handle_call({:terminate_child, child_pid} = msg, from, state) do
    # Replication can select another partition's copy of the same logical ID.
    # A caller naming our actual child must still be able to terminate it.
    local_child =
      Enum.find_value(state.local_processes, fn
        {_id, {spec, ^child_pid}} -> spec
        _ -> nil
      end)

    if local_child do
      {reply, state} = terminate_child(local_child, state)
      {:reply, reply, state}
    else
      with child_id when not is_nil(child_id) <- get_item(state.process_pid_to_id, child_pid),
           {other_node, _child, _pid} <- get_item(state.processes_by_id, child_id) do
        if other_node == fully_qualified_name(state.name),
          do: {:reply, {:error, :not_found}, state},
          else: proxy_to_node(other_node, msg, from, state)
      else
        nil -> {:reply, {:error, :not_found}, state}
      end
    end
  end

  def handle_call({:start_child, _child_spec}, _from, %{shutting_down: true} = state),
    do: {:reply, {:error, {:shutting_down, "this node is shutting down."}}, state}

  def handle_call({:start_child, child_spec} = msg, from, state) do
    this_name = fully_qualified_name(state.name)
    proxy_ttl_expired? = proxy_message_ttl(state, from) == 0

    child_spec = randomize_child_id(child_spec)

    case choose_node(child_spec, state) do
      {:ok, %{name: node_name}} when node_name == this_name or proxy_ttl_expired? ->
        {reply, new_state} = add_child(child_spec, state)
        {:reply, reply, new_state}

      {:ok, %{name: other_node_name}} ->
        proxy_to_node(other_node_name, msg, from, state)

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:which_children, _from, state) do
    which_children =
      Enum.flat_map(members(state), fn
        {_, %{name: {name, node}}} ->
          [{supervisor_name(name), node}]
      end)
      |> Enum.flat_map(fn supervisor_name ->
        try do
          Horde.ProcessesSupervisor.which_children(supervisor_name)
        catch
          :exit, _ -> []
        end
      end)
      |> Enum.map(fn {_id, pid, type, module} -> {:undefined, pid, type, module} end)

    {:reply, which_children, state}
  end

  def handle_call(:count_children, _from, state) do
    count =
      Enum.flat_map(members(state), fn
        {_, %{name: {name, node}}} ->
          [{supervisor_name(name), node}]
      end)
      |> Enum.flat_map(fn supervisor_name ->
        try do
          Horde.ProcessesSupervisor.count_children(supervisor_name)
        catch
          :exit, _ -> [nil]
        end
      end)
      |> Enum.reject(fn
        nil -> true
        _ -> false
      end)
      |> Enum.reduce(%{}, fn {process_type, count}, acc ->
        Map.update(acc, process_type, count, &(&1 + count))
      end)

    {:reply, count, state}
  end

  def handle_cast(
        {:processes_supervisor_ready, pid},
        %{processes_supervisor_ready: false, shutting_down: false} = state
      ) do
    if Process.whereis(supervisor_name(state.name)) == pid do
      # Refresh once before publishing readiness. Notifications sent before this
      # process existed may have been dropped, and others can still be queued.
      state = load_initial_state(state)
      state = %{state | processes_supervisor_ready: true}

      state =
        state
        |> set_own_node_status()
        |> monitor_supervisors()
        |> publish_observation()
        |> handle_quorum_change()

      # Records from an earlier local incarnation do not prove that this new
      # local supervisor owns a child. Relinquish them conditionally, preserving
      # their specifications for ordinary placement without adopting old PIDs.
      owner = fully_qualified_name(state.name)

      for {^owner, spec, old_pid} <- all_items_values(state.processes_by_id),
          not Map.has_key?(state.local_processes, spec.id) do
        Horde.DynamicSupervisorCrdt.relinquish_owned(
          crdt_name(state.name),
          spec.id,
          old_pid,
          spec
        )
      end

      {:noreply, handoff_processes(state)}
    else
      {:noreply, state}
    end
  end

  def handle_cast({:processes_supervisor_ready, _pid}, state), do: {:noreply, state}

  def handle_cast({:update_child_pid, child_id, new_pid}, state) do
    {:noreply, set_child_pid(state, child_id, new_pid)}
  end

  def handle_cast({:relinquish_child_process, child_id}, state) do
    # signal to the rest of the nodes that this process has been relinquished
    # (to the Horde!) by its parent
    with {child, pid} <- Map.get(state.local_processes, child_id) do
      Horde.DynamicSupervisorCrdt.relinquish_owned(crdt_name(state.name), child.id, pid, child)
    end

    {:noreply, forget_local_process(state, child_id)}
  end

  # TODO think of a better name than "disown_child_process"
  def handle_cast({:disown_child_process, child_id}, state) do
    case Map.get(state.local_processes, child_id) do
      {_spec, pid} ->
        state = forget_local_process(state, child_id)
        Horde.DynamicSupervisorCrdt.drop_owned(crdt_name(state.name), child_id, pid)
        {:noreply, state}

      nil ->
        {:noreply, state}
    end
  end

  defp set_child_pid(state, child_id, new_child_pid) do
    case Map.get(state.local_processes, child_id) do
      {child_spec, old_pid} ->
        name = fully_qualified_name(state.name)

        DeltaCrdt.put(
          crdt_name(state.name),
          {:process, child_spec.id},
          {fully_qualified_name(state.name), child_spec, new_child_pid},
          :infinity
        )

        new_processes_by_id =
          put_item(state.processes_by_id, child_id, {name, child_spec, new_child_pid})

        new_process_pid_to_id =
          put_item(state.process_pid_to_id, new_child_pid, child_id) |> delete_item(old_pid)

        %{
          state
          | processes_by_id: new_processes_by_id,
            process_pid_to_id: new_process_pid_to_id,
            local_processes: Map.put(state.local_processes, child_id, {child_spec, new_child_pid})
        }

      nil ->
        state
    end
  end

  @big_number round(:math.pow(2, 128))

  defp randomize_child_id(child) do
    Map.put(child, :id, :rand.uniform(@big_number))
  end

  defp proxy_to_node(_node_name, message, reply_to, %{proxy_operation_ttl: {reply_to, 0}} = state) do
    message_type = elem(message, 0)

    {:reply,
     {:error, :proxy_operation_ttl_expired,
      "a proxied #{message_type} message was discard because its TTL expired"}, state}
  end

  defp proxy_to_node(node_name, message, reply_to, state) do
    case Map.get(members(state), node_name) do
      %{status: :alive} ->
        Horde.ProxyOperation.forward(
          node_name,
          message,
          reply_to,
          proxy_message_ttl(state, reply_to)
        )

        {:noreply, state}

      _ ->
        {:reply,
         {:error,
          {:node_dead_or_shutting_down,
           "the node responsible for this process is shutting down or dead, try again soon"}},
         state}
    end
  end

  defp proxy_message_ttl(%{proxy_operation_ttl: {reply_to, ttl}} = _state, reply_to), do: ttl
  defp proxy_message_ttl(%{proxy_message_ttl: ttl} = _state, _reply_to), do: ttl

  defp decrement_ttl(:infinity), do: :infinity
  defp decrement_ttl(n) when is_integer(n), do: n - 1

  defp set_own_node_status(state, force \\ false)

  defp set_own_node_status(state, false) do
    if Map.get(state.members_info, fully_qualified_name(state.name)) == node_info(state) do
      state
    else
      set_own_node_status(state, true)
    end
  end

  defp set_own_node_status(state, true) do
    DeltaCrdt.put(
      crdt_name(state.name),
      {:member_node_info, fully_qualified_name(state.name)},
      node_info(state),
      :infinity
    )

    new_members_info =
      Map.put(state.members_info, fully_qualified_name(state.name), node_info(state))

    Map.put(state, :members_info, new_members_info)
  end

  def handle_info({:set_members, members}, state) do
    {:noreply, set_members(members, state)}
  end

  def handle_info({:proxy_operation, msg, reply_to}, state) do
    handle_info({:proxy_operation, msg, reply_to, :infinity}, state)
  end

  def handle_info({:proxy_operation, msg, reply_to, ttl}, state) do
    state = %{state | proxy_operation_ttl: {reply_to, decrement_ttl(ttl)}}

    case handle_call(msg, reply_to, state) do
      {:reply, reply, new_state} ->
        GenServer.reply(reply_to, reply)
        {:noreply, new_state}

      {:noreply, new_state} ->
        {:noreply, new_state}
    end
  end

  def handle_info({:DOWN, ref, _type, _pid, reason}, state) do
    case Map.get(state.supervisor_ref_to_name, ref) do
      nil ->
        {:noreply, state}

      name ->
        # A monitor reports this observer's reachability. In an overlapping
        # partition, another member may still reach the same live supervisor.
        # Publishing :dead would make those observers repeatedly contradict
        # the owner's :alive record and redistribute healthy processes.
        new_state =
          forget_monitor(state, name)
          |> Map.put(:unavailable_members, Map.put(state.unavailable_members, name, reason))
          |> publish_observation()
          |> handle_quorum_change()
          |> handoff_processes()

        {:noreply, new_state}
    end
  end

  def handle_info({:nodeup, node, _info}, state) do
    state =
      Enum.reduce(state.members, state, fn
        {{_name, ^node} = member, _}, state ->
          forget_monitor(state, member)
          |> Map.update!(:unavailable_members, &Map.delete(&1, member))

        _, state ->
          state
      end)

    {:noreply,
     state
     |> monitor_supervisors()
     |> publish_observation()
     |> handle_quorum_change()
     |> handoff_processes()}
  end

  def handle_info({:nodedown, node, _info}, state) do
    state =
      Enum.reduce(state.members, state, fn
        {{_name, ^node} = member, _}, state ->
          forget_monitor(state, member)
          |> Map.update!(:unavailable_members, &Map.put(&1, member, :noconnection))

        _, state ->
          state
      end)

    {:noreply, state |> publish_observation() |> handle_quorum_change() |> handoff_processes()}
  end

  def handle_info(:retry_recoveries, state) do
    processes =
      Enum.flat_map(state.pending_recoveries, fn {id, _reason} ->
        case get_item(state.processes_by_id, id) do
          nil -> []
          process -> [process]
        end
      end)

    {:noreply, handoff_processes(%{state | recovery_retry_timer: nil}, true, processes)}
  end

  @doc false
  def handle_info({:processes_updated, reply_to}, %{shutting_down: true} = state) do
    GenServer.reply(reply_to, :ok)
    {:noreply, state}
  end

  def handle_info({:crdt_update, diffs}, state) do
    # Notifications can predate bootstrap or a newer local lifecycle operation.
    # Reconcile only their keys against the current CRDT, never replay old values.
    keys = Enum.map(diffs, &elem(&1, 1)) |> Enum.uniq()
    current = DeltaCrdt.take(crdt_name(state.name), keys, :infinity)

    diffs =
      Enum.map(keys, fn key ->
        case Map.fetch(current, key) do
          {:ok, value} -> {:add, key, value}
          :error -> {:remove, key}
        end
      end)

    new_state =
      update_members(state, diffs)
      |> set_own_node_status()
      |> update_processes(diffs)

    new_state =
      if has_membership_changed?(diffs) do
        monitor_supervisors(new_state)
        |> set_own_node_status()
        |> publish_observation()
        |> handle_quorum_change()
        |> set_crdt_neighbours()
        |> handoff_processes()
      else
        # Specifications can arrive after the monitor that detected their
        # owner disappearing. Only inspect those changed records here: scanning
        # every child on every registration makes ordinary starts quadratic.
        changed_processes =
          Enum.flat_map(diffs, fn
            {:add, {:process, id}, {_owner, _spec, _pid}} ->
              case get_item(new_state.processes_by_id, id) do
                nil -> []
                process -> [process]
              end

            _ ->
              []
          end)

        handoff_processes(new_state, false, changed_processes)
      end

    {:noreply, new_state}
  end

  def has_membership_changed?([{:add, {:member_node_info, _}, _} = _diff | _diffs]), do: true
  def has_membership_changed?([{:remove, {:member_node_info, _}} = _diff | _diffs]), do: true
  def has_membership_changed?([{:add, {:member, _}, _} = _diff | _diffs]), do: true
  def has_membership_changed?([{:remove, {:member, _}} = _diff | _diffs]), do: true
  def has_membership_changed?([{:add, {:member_observation, _}, _} | _]), do: true
  def has_membership_changed?([{:remove, {:member_observation, _}} | _]), do: true

  def has_membership_changed?([_diff | diffs]) do
    has_membership_changed?(diffs)
  end

  def has_membership_changed?([]), do: false

  defp handoff_processes(state, retry? \\ false, processes \\ nil)

  defp handoff_processes(%{processes_supervisor_ready: false} = state, _retry?, _processes),
    do: state

  defp handoff_processes(%{shutting_down: true} = state, _retry?, _processes), do: state

  defp handoff_processes(state, retry?, processes) do
    this_node = fully_qualified_name(state.name)

    (processes || all_items_values(state.processes_by_id))
    |> Enum.reduce(state, fn {current_node, child_spec, _child_pid}, state ->
      {state, current_node} = restore_existing_local_owner(state, current_node, child_spec)

      case choose_node(child_spec, state) do
        {:ok, %{name: chosen_node}} ->
          case {current_node, chosen_node} do
            {same_node, same_node} ->
              # process is running on the node on which it belongs

              clear_pending_recovery(state, child_spec.id)

            {^this_node, _other_node} ->
              # process is running here but belongs somewhere else

              case state.supervisor_options[:process_redistribution] do
                :active ->
                  handoff_child(child_spec, state)

                :passive ->
                  clear_pending_recovery(state, child_spec.id)
              end

            {_current_node, ^this_node} ->
              # process is running on another node but belongs here

              if owner_unreachable?(state, current_node) do
                if retry? or not Map.has_key?(state.pending_recoveries, child_spec.id) do
                  recover_child(child_spec, state)
                else
                  state
                end
              else
                clear_pending_recovery(state, child_spec.id)
              end

            {_other_node1, _other_node2} ->
              # process is neither running here nor belongs here

              clear_pending_recovery(state, child_spec.id)
          end

        {:error, _reason} ->
          state
      end
    end)
    |> schedule_recovery_retry()
  end

  defp owner_unreachable?(state, owner) do
    case Map.get(members(state), owner) do
      nil -> true
      %{status: :dead} -> not MapSet.member?(observed_reachable_members(state), owner)
      _ -> false
    end
  end

  defp restore_existing_local_owner(state, owner, child_spec) do
    this_node = fully_qualified_name(state.name)

    case Map.get(state.local_processes, child_spec.id) do
      {local_spec, pid} when owner != this_node ->
        # A replacement can disappear before publishing its losing copy's
        # cleanup. Preserve the already-live original even when some third
        # member would be selected to start a new copy of this specification.
        if Process.alive?(pid) and owner_unreachable?(state, owner),
          do: {update_state_with_child(local_spec, pid, state), this_node},
          else: {state, owner}

      _ ->
        {state, owner}
    end
  end

  defp recover_child(_spec, %{processes_supervisor_ready: false} = state), do: state
  defp recover_child(_spec, %{shutting_down: true} = state), do: state

  defp recover_child(child_spec, state) do
    {_owner, _spec, previous_pid} = get_item(state.processes_by_id, child_spec.id)

    {response, state} =
      case Map.get(state.local_processes, child_spec.id) do
        {_spec, pid} ->
          if Process.alive?(pid),
            do: {{:ok, pid}, update_state_with_child(child_spec, pid, state)},
            else: {{:error, :local_child_restarting}, state}

        nil ->
          # Keep the logical ID across takeover. Otherwise a late graceful
          # relinquishment can start a second replacement under the old ID.
          add_child(child_spec, state)
      end

    case response do
      {:error, reason} ->
        # Keep the old specification until a replacement actually exists. A
        # failed start can mean that the previous owner is still running.
        if Map.get(state.pending_recoveries, child_spec.id) != reason do
          Logger.warning("Unable to recover child #{inspect(child_spec.id)}: #{inspect(reason)}")
        end

        %{state | pending_recoveries: Map.put(state.pending_recoveries, child_spec.id, reason)}

      :ignore ->
        # :ignore explicitly declines the child, as in DynamicSupervisor.
        # A duplicate start must return {:error, {:already_started, pid}} if
        # the original recovery obligation should be retained.
        if is_pid(previous_pid),
          do:
            Horde.DynamicSupervisorCrdt.drop_owned(
              crdt_name(state.name),
              child_spec.id,
              previous_pid
            ),
          else:
            Horde.DynamicSupervisorCrdt.drop_relinquished(crdt_name(state.name), child_spec.id)

        update_process(state, {:remove, {:process, child_spec.id}})

      success when elem(success, 0) == :ok ->
        clear_pending_recovery(state, child_spec.id)
    end
  end

  defp clear_pending_recovery(state, id),
    do: %{state | pending_recoveries: Map.delete(state.pending_recoveries, id)}

  defp schedule_recovery_retry(%{recovery_retry_timer: nil, pending_recoveries: pending} = state)
       when map_size(pending) > 0 do
    # This retries a known failed local start, not an uncertain remote call and
    # not a timeout-based declaration that another node has died.
    timer = Process.send_after(self(), :retry_recoveries, @recovery_retry_interval)
    %{state | recovery_retry_timer: timer}
  end

  defp schedule_recovery_retry(state), do: state

  defp update_processes(state, [diff | diffs]) do
    update_process(state, diff)
    |> update_processes(diffs)
  end

  defp update_processes(state, []), do: state

  defp update_process(state, {:add, {:process, child_id}, {nil, child_spec}}) do
    this_name = fully_qualified_name(state.name)

    case Map.get(state.local_processes, child_id) do
      {_spec, _pid} ->
        restore_local_process(state, child_id)

      nil ->
        state = update_process(state, {:add, {:process, child_id}, {nil, child_spec, nil}})

        case choose_node(child_spec, state) do
          {:ok, %{name: ^this_name}} -> recover_child(child_spec, state)
          _ -> state
        end
    end
  end

  defp update_process(state, {:add, {:process, child_id}, {node, child_spec, child_pid}}) do
    new_process_pid_to_id =
      case get_item(state.processes_by_id, child_id) do
        {_, _, old_pid} -> delete_item(state.process_pid_to_id, old_pid)
        nil -> state.process_pid_to_id
      end

    new_process_pid_to_id =
      if is_pid(child_pid),
        do: put_item(new_process_pid_to_id, child_pid, child_id),
        else: new_process_pid_to_id

    new_processes_by_id = put_item(state.processes_by_id, child_id, {node, child_spec, child_pid})

    Map.put(state, :processes_by_id, new_processes_by_id)
    |> Map.put(:process_pid_to_id, new_process_pid_to_id)
    |> clear_pending_recovery(child_id)
  end

  defp update_process(state, {:remove, {:process, child_id}}) do
    case Map.get(state.local_processes, child_id) do
      {_spec, pid} when is_pid(pid) ->
        if Process.alive?(pid) do
          restore_local_process(state, child_id)
        else
          remove_process_record(state, child_id)
        end

      nil ->
        remove_process_record(state, child_id)
    end
  end

  defp update_process(state, _), do: state

  defp restore_local_process(%{shutting_down: true} = state, _id), do: state

  defp restore_local_process(state, id) do
    case Map.get(state.local_processes, id) do
      {spec, pid} ->
        if Process.alive?(pid) do
          Horde.DynamicSupervisorCrdt.restore_owned(
            crdt_name(state.name),
            id,
            {fully_qualified_name(state.name), spec, pid}
          )
        end

        state

      nil ->
        state
    end
  end

  defp remove_process_record(state, child_id) do
    {value, new_processes_by_id} = pop_item(state.processes_by_id, child_id)

    new_process_pid_to_id =
      case value do
        {_node_name, _child_spec, child_pid} ->
          delete_item(state.process_pid_to_id, child_pid)

        nil ->
          state.process_pid_to_id
      end

    Map.put(state, :processes_by_id, new_processes_by_id)
    |> Map.put(:process_pid_to_id, new_process_pid_to_id)
    |> clear_pending_recovery(child_id)
  end

  defp update_members(state, [diff | diffs]) do
    update_member(state, diff)
    |> update_members(diffs)
  end

  defp update_members(state, []), do: state

  defp update_member(state, {:add, {:member, member}, 1}) do
    new_members = Map.put_new(state.members, member, 1)
    new_members_info = Map.put_new(state.members_info, member, uninitialized_member(member))

    Map.put(state, :members, new_members)
    |> Map.put(:members_info, new_members_info)
  end

  defp update_member(state, {:remove, {:member, member}}) do
    new_members = Map.delete(state.members, member)

    forget_monitor(state, member)
    |> Map.put(:members, new_members)
    |> Map.update!(:unavailable_members, &Map.delete(&1, member))
  end

  defp update_member(state, {:add, {:member_observation, member}, observation}),
    do: %{state | member_observations: Map.put(state.member_observations, member, observation)}

  defp update_member(state, {:remove, {:member_observation, member}}),
    do: %{state | member_observations: Map.delete(state.member_observations, member)}

  defp update_member(state, {:add, {:member_node_info, member}, node_info}) do
    previous = Map.get(state.members_info, member)

    state =
      if is_pid(Map.get(node_info, :pid)) and
           Map.get(previous || %{}, :pid) != node_info.pid do
        # The owner's PID includes its VM incarnation. A restart on the same
        # connected node must invalidate the previous monitor and suspicion.
        forget_monitor(state, member)
        |> Map.update!(:unavailable_members, &Map.delete(&1, member))
      else
        state
      end

    new_members = Map.put(state.members_info, member, node_info)

    Map.put(state, :members_info, new_members)
  end

  defp update_member(state, {:remove, {:member_node_info, member}}) do
    new_members = Map.delete(state.members_info, member)

    Map.put(state, :members_info, new_members)
  end

  defp update_member(state, _), do: state

  defp uninitialized_member(member) do
    %Horde.DynamicSupervisor.Member{status: :uninitialized, name: member}
  end

  defp member_names(names) do
    Enum.map(names, fn
      {name, node} -> {name, node}
      name when is_atom(name) -> {name, node()}
    end)
  end

  defp set_members(members, state) do
    members = Enum.map(members, &fully_qualified_name/1)

    uninitialized_new_members_info =
      member_names(members)
      |> Map.new(fn name ->
        {name, %Horde.DynamicSupervisor.Member{name: name, status: :uninitialized}}
      end)

    new_members_info =
      Map.merge(
        uninitialized_new_members_info,
        Map.take(state.members_info, Map.keys(uninitialized_new_members_info))
      )

    new_members = Map.new(new_members_info, fn {member, _} -> {member, 1} end)

    new_member_names = Map.keys(new_members_info) |> MapSet.new()
    existing_member_names = Map.keys(state.members) |> MapSet.new()

    state =
      MapSet.difference(existing_member_names, new_member_names)
      |> Enum.reduce(state, &forget_monitor(&2, &1))
      |> Map.update!(:unavailable_members, &Map.take(&1, Map.keys(new_members)))

    keys_to_remove =
      MapSet.difference(existing_member_names, new_member_names)
      |> Enum.flat_map(fn removed_member ->
        [
          {:member, removed_member},
          {:member_node_info, removed_member},
          {:member_observation, removed_member}
        ]
      end)

    DeltaCrdt.drop(
      crdt_name(state.name),
      keys_to_remove,
      :infinity
    )

    map_to_add =
      MapSet.difference(new_member_names, existing_member_names)
      |> Map.new(fn added_member -> {{:member, added_member}, 1} end)

    DeltaCrdt.merge(crdt_name(state.name), map_to_add, :infinity)

    %{state | members: new_members, members_info: new_members_info}
    |> monitor_supervisors()
    |> publish_observation()
    |> handle_quorum_change()
    |> set_crdt_neighbours()
    |> handoff_processes()
  end

  defp handle_quorum_change(state) do
    if state.distribution_strategy.has_quorum?(Map.values(members(state))) do
      Enum.each(state.waiting_for_quorum, fn from -> GenServer.reply(from, :ok) end)
      %{state | waiting_for_quorum: []}
    else
      shut_down_all_processes(state)
    end
  end

  defp shut_down_all_processes(state) do
    if map_size(state.local_processes) > 0 do
      :ok = Horde.ProcessesSupervisor.stop(supervisor_name(state.name))
    end

    state
  end

  defp set_crdt_neighbours(state) do
    names = Map.keys(state.members) -- [fully_qualified_name(state.name)]

    crdt_names = Enum.map(names, fn {name, node} -> {crdt_name(name), node} end)

    send(crdt_name(state.name), {:set_neighbours, crdt_names})

    state
  end

  defp monitor_supervisors(state) do
    new_supervisor_refs =
      Enum.flat_map(members(state), fn
        {name, %{status: :alive} = member} ->
          [{name, Map.get(member, :pid) || name}]

        {name, %{status: :uninitialized, pid: pid}} when is_pid(pid) ->
          # A startup failure can leave the BEAM connected. Observe this known
          # incarnation so earlier child obligations still fail over if it dies;
          # :uninitialized remains ineligible for placement until readiness.
          [{name, pid}]

        _ ->
          []
      end)
      |> Enum.reject(fn {name, _target} ->
        Map.has_key?(state.name_to_supervisor_ref, name)
      end)
      |> Map.new(fn {name, target} ->
        {name, Process.monitor(target)}
      end)

    new_supervisor_ref_to_name =
      Map.merge(
        state.supervisor_ref_to_name,
        Map.new(new_supervisor_refs, fn {k, v} -> {v, k} end)
      )

    new_name_to_supervisor_ref = Map.merge(state.name_to_supervisor_ref, new_supervisor_refs)

    Map.put(state, :supervisor_ref_to_name, new_supervisor_ref_to_name)
    |> Map.put(:name_to_supervisor_ref, new_name_to_supervisor_ref)
  end

  defp forget_monitor(state, member) do
    case Map.pop(state.name_to_supervisor_ref, member) do
      {nil, _} ->
        state

      {ref, names} ->
        Process.demonitor(ref, [:flush])

        %{
          state
          | name_to_supervisor_ref: names,
            supervisor_ref_to_name: Map.delete(state.supervisor_ref_to_name, ref)
        }
    end
  end

  defp publish_observation(state) do
    member = fully_qualified_name(state.name)

    reachable =
      Map.take(state.members_info, Map.keys(state.name_to_supervisor_ref))
      |> Map.new(fn {name, info} -> {name, Map.get(info, :pid)} end)

    observation = %{pid: self(), reachable: reachable}

    if Map.get(state.member_observations, member) == observation do
      state
    else
      DeltaCrdt.put(crdt_name(state.name), {:member_observation, member}, observation, :infinity)
      %{state | member_observations: Map.put(state.member_observations, member, observation)}
    end
  end

  defp observed_reachable_members(state) do
    # Anchor the traversal in direct live observations. Disconnected components'
    # old views cannot keep one another alive. Follow validated incarnations so
    # a chain A--B--C--D can witness D without requiring a direct A--D link.
    roots =
      members(state)
      |> Enum.flat_map(fn {name, info} -> if info.status == :alive, do: [name], else: [] end)

    observation_closure(roots, MapSet.new(), state)
  end

  defp observation_closure([], seen, _state), do: seen

  defp observation_closure([member | remaining], seen, state) do
    if MapSet.member?(seen, member) do
      observation_closure(remaining, seen, state)
    else
      targets =
        with %{pid: pid, reachable: reachable} when is_pid(pid) <-
               state.member_observations[member],
             %{pid: ^pid} <- state.members_info[member] do
          Enum.flat_map(reachable, fn {target, incarnation} ->
            if Map.has_key?(state.members, target) and is_pid(incarnation) and
                 Map.get(state.members_info[target] || %{}, :pid) == incarnation,
               do: [target],
               else: []
          end)
        else
          _ -> []
        end

      observation_closure(remaining ++ targets, MapSet.put(seen, member), state)
    end
  end

  defp update_state_with_child(child, child_pid, state) do
    DeltaCrdt.put(
      crdt_name(state.name),
      {:process, child.id},
      {fully_qualified_name(state.name), child, child_pid},
      :infinity
    )

    new_processes_by_id =
      put_item(
        state.processes_by_id,
        child.id,
        {fully_qualified_name(state.name), child, child_pid}
      )

    new_process_pid_to_id = put_item(state.process_pid_to_id, child_pid, child.id)
    local_processes = Map.put(state.local_processes, child.id, {child, child_pid})

    Map.put(state, :processes_by_id, new_processes_by_id)
    |> Map.put(:process_pid_to_id, new_process_pid_to_id)
    |> Map.put(:local_processes, local_processes)
    |> Map.put(:local_process_count, map_size(local_processes))
  end

  defp handoff_child(child, state) do
    case get_item(state.processes_by_id, child.id) do
      {_, _, child_pid} ->
        # we send a special exit signal to the process here.
        # when the process has exited, Horde.ProcessSupervisor
        # will cast `{:relinquish_child_process, child_id}`
        # to this process for cleanup.

        Horde.ProcessesSupervisor.send_exit_signal(
          supervisor_name(state.name),
          child_pid,
          {:shutdown, :process_redistribution}
        )

        state

      nil ->
        state
    end
  end

  defp terminate_child(child, state) do
    child_id = child.id
    {_spec, pid} = Map.fetch!(state.local_processes, child_id)

    reply =
      Horde.ProcessesSupervisor.terminate_child_by_id(
        supervisor_name(state.name),
        child_id
      )

    new_state = forget_local_process(state, child_id)
    Horde.DynamicSupervisorCrdt.drop_owned(crdt_name(state.name), child_id, pid)

    {reply, new_state}
  end

  defp forget_local_process(state, id) do
    {local, remaining} = Map.pop(state.local_processes, id)

    case {local, get_item(state.processes_by_id, id)} do
      {{_spec, pid}, {_member, _recorded_spec, pid}} ->
        delete_item(state.processes_by_id, id)
        delete_item(state.process_pid_to_id, pid)

      _ ->
        :ok
    end

    %{state | local_processes: remaining, local_process_count: map_size(remaining)}
  end

  defp add_child(child, state) do
    {[response], new_state} = add_children([child], state)
    {response, new_state}
  end

  defp add_children(children, state) do
    Enum.map(children, fn child_spec ->
      case Horde.ProcessesSupervisor.start_child(supervisor_name(state.name), child_spec) do
        {:ok, child_pid} ->
          {{:ok, child_pid}, child_spec}

        {:ok, child_pid, term} ->
          {{:ok, child_pid, term}, child_spec}

        {:error, error} ->
          {:error, error}

        :ignore ->
          :ignore
      end
    end)
    |> Enum.reduce({[], state}, fn
      {{:ok, child_pid} = resp, child_spec}, {responses, state} ->
        {[resp | responses], update_state_with_child(child_spec, child_pid, state)}

      {{:ok, child_pid, _term} = resp, child_spec}, {responses, state} ->
        {[resp | responses], update_state_with_child(child_spec, child_pid, state)}

      {:error, error}, {responses, state} ->
        {[{:error, error} | responses], state}

      :ignore, {responses, state} ->
        {[:ignore | responses], state}
    end)
  end

  defp choose_node(child_spec, state) do
    state.distribution_strategy.choose_node(
      child_spec,
      Map.values(members(state))
    )
  end

  defp members(state) do
    Map.take(state.members_info, Map.keys(state.members))
    |> Map.new(fn {name, member} ->
      if Map.has_key?(state.unavailable_members, name),
        do: {name, %{member | status: :dead}},
        else: {name, member}
    end)
  end
end
