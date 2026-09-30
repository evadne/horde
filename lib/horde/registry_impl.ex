defmodule Horde.RegistryImpl do
  @moduledoc false

  use GenServer

  require Logger

  defmodule State do
    @moduledoc false
    # Local intent outlives replicated visibility, but never its owner or an
    # explicit unregister/resolved conflict. It is not a replicated membership claim.
    defstruct name: nil,
              nodes: MapSet.new(),
              members: MapSet.new(),
              registry_ets_table: nil,
              pids_ets_table: nil,
              keys_ets_table: nil,
              members_ets_table: nil,
              local_registrations: %{},
              listeners: []
  end

  @spec child_spec(options :: list()) :: Supervisor.child_spec()
  def child_spec(options \\ []) do
    %{
      id: Keyword.get(options, :name, __MODULE__),
      start: {__MODULE__, :start_link, [options]}
    }
  end

  @spec start_link(options :: list()) :: GenServer.on_start()
  def start_link(options \\ []) do
    name = Keyword.get(options, :name)

    if !is_atom(name) || is_nil(name) do
      raise ArgumentError, "expected :name to be given and to be an atom, got: #{inspect(name)}"
    end

    GenServer.start_link(__MODULE__, options, name: name)
  end

  def on_diffs(name, diffs) do
    try do
      Kernel.send(name, {:crdt_update, diffs})
    rescue
      ArgumentError ->
        # the process might already been stopped
        :ok
    end
  end

  ### GenServer callbacks

  def init(opts) do
    Process.flag(:trap_exit, true)

    name = Keyword.get(opts, :name)
    pids_name = :"pids_#{name}"
    keys_name = :"keys_#{name}"
    members_name = :"members_#{name}"
    listeners = Keyword.get(opts, :listeners, [])

    unless is_list(listeners) and Enum.all?(listeners, &is_atom/1) do
      raise ArgumentError,
            "expected :listeners to be a list of named processes, got: #{inspect(listeners)}"
    end

    Logger.info("Starting #{inspect(__MODULE__)} with name #{inspect(name)}")

    unless is_atom(name) do
      raise ArgumentError, "expected :name to be given and to be an atom, got: #{inspect(name)}"
    end

    :ets.new(name, [:named_table, {:read_concurrency, true}])
    :ets.new(pids_name, [:named_table, {:read_concurrency, true}])
    :ets.new(keys_name, [:named_table, {:read_concurrency, true}])
    :ets.new(members_name, [:named_table, {:read_concurrency, true}])

    state = %State{
      name: name,
      registry_ets_table: name,
      pids_ets_table: pids_name,
      keys_ets_table: keys_name,
      members_ets_table: members_name,
      listeners: listeners
    }

    state =
      case Keyword.get(opts, :members) do
        nil ->
          state

        :auto ->
          state.name
          |> Horde.NodeListener.make_members()
          |> set_initial_members(state)

        members ->
          set_initial_members(members, state)
      end

    case Keyword.get(opts, :meta) do
      nil ->
        nil

      meta ->
        Enum.each(meta, fn {key, value} -> put_meta(state, key, value) end)
    end

    {:ok, state}
  end

  def handle_info({:crdt_update, diffs}, state) do
    new_state = process_diffs(state, diffs)
    {:noreply, new_state}
  end

  def handle_info({:EXIT, pid, _reason}, state) do
    retained_keys = for {key, {^pid, _value}} <- state.local_registrations, do: key
    state = %{state | local_registrations: Map.drop(state.local_registrations, retained_keys)}

    visible_keys =
      case :ets.take(state.pids_ets_table, pid) do
        [{_pid, keys}] -> keys
        [] -> []
      end

    keys = Enum.uniq(retained_keys ++ visible_keys)
    Horde.RegistryCrdt.drop_owned(crdt_name(state.name), keys, pid)
    Enum.each(keys, &unregister_local(state, &1, pid))

    {:noreply, state}
  end

  def set_initial_members(members, state) do
    members = Enum.map(members, &fully_qualified_name/1)

    DeltaCrdt.merge(
      crdt_name(state.name),
      Map.new(members, fn member -> {{:member, member}, 1} end),
      :infinity
    )

    # Replication can populate the CRDT before this named process exists, so
    # its first notifications may have had no recipient. An equal-value merge
    # emits no replacement diffs: materialise the authoritative snapshot here.
    # Membership comes first; normal reconciliation reads current CRDT values
    # again so queued changes cannot revive an obsolete registration.
    initial_diffs =
      DeltaCrdt.to_map(crdt_name(state.name))
      |> Enum.sort_by(fn
        {{:member, _}, _} -> 0
        _ -> 1
      end)
      |> Enum.map(fn {key, value} -> {:add, key, value} end)

    process_diffs(state, initial_diffs)
  end

  defp process_diffs(state, [diff | diffs]) do
    process_diff(state, diff)
    |> process_diffs(diffs)
  end

  defp process_diffs(state, []), do: state

  defp process_diff(state, {:add, {:member, member}, 1}) do
    reconcile_member(state, member)
  end

  defp process_diff(state, {:remove, {:member, member}}) do
    reconcile_member(state, member)
  end

  defp process_diff(state, {:remove, {:registry, key}}) do
    :ets.delete(state.name, key)
    state
  end

  defp process_diff(state, {:add, {:key, key}, _registration}) do
    reconcile_registration(state, key)
  end

  defp process_diff(state, {:remove, {:key, key}}) do
    reconcile_registration(state, key)
  end

  defp process_diff(state, {:add, {:registry, key}, value}) do
    :ets.insert(state.registry_ets_table, {key, value})
    state
  end

  defp reconcile_member(state, member) do
    case DeltaCrdt.get(crdt_name(state.name), {:member, member}) do
      1 -> add_member(state, member)
      nil -> remove_member(state, member)
    end
  end

  defp add_member(state, member) do
    new_members = MapSet.put(state.members, member)

    :ets.insert(state.members_ets_table, {member, 1})

    neighbours =
      MapSet.delete(new_members, fully_qualified_name(state.name))
      |> crdt_names()

    send(crdt_name(state.name), {:set_neighbours, neighbours})

    new_nodes = Enum.map(new_members, fn {_name, node} -> node end) |> MapSet.new()

    %{state | members: new_members, nodes: new_nodes}
    |> restore_local_registrations()
  end

  defp remove_member(state, member) do
    :ets.match_delete(state.members_ets_table, {member, 1})

    removed_keys = :ets.match(state.keys_ets_table, {:"$1", member, {:"$2", :_}})

    Horde.RegistryCrdt.drop_member(crdt_name(state.name), member, Enum.map(removed_keys, &hd/1))

    Enum.each(removed_keys, fn [key, pid] ->
      unregister_local(state, key, pid)
    end)

    new_members = MapSet.delete(state.members, member)
    new_nodes = Enum.map(new_members, fn {_name, node} -> node end) |> MapSet.new()

    %{state | members: new_members, nodes: new_nodes}
  end

  defp reconcile_registration(state, key) do
    # Read the authoritative local CRDT: queued diffs may predate a newer local
    # registration or a membership change. They must not retire a newer intent.
    case DeltaCrdt.get(crdt_name(state.name), {:key, key}) do
      nil ->
        unregister_local(state, key)
        restore_local_registrations(state, [key])

      {member, pid, _value} = registration ->
        case DeltaCrdt.get(crdt_name(state.name), {:member, member}) do
          1 ->
            accept_registration(state, key, registration)

          nil ->
            Horde.RegistryCrdt.drop_member(crdt_name(state.name), member, [key])
            unregister_local(state, key, pid)
            restore_local_registrations(state, [key])
        end
    end
  end

  defp accept_registration(state, key, {member, pid, value}) do
    already_visible? = :ets.lookup(state.keys_ets_table, key) == [{key, member, {pid, value}}]
    link_local_pid(pid)
    add_key_to_pids_table(state, pid, key)

    visible_owners =
      for {^key, _member, owner} <- :ets.lookup(state.keys_ets_table, key), do: owner

    retained_owners =
      case Map.fetch(state.local_registrations, key) do
        {:ok, owner} -> [owner]
        :error -> []
      end

    state =
      (visible_owners ++ retained_owners)
      |> Enum.uniq_by(&elem(&1, 0))
      |> Enum.reject(fn {owner, _value} -> owner == pid end)
      |> Enum.reduce(state, fn {other_pid, other_value}, state ->
        # Retire the losing intent before notifications. A process trapping the
        # conflict exit must never reassert its claim on a later membership event.
        state = retire_registration(state, key, other_pid)
        unregister_local(state, key, other_pid)
        Process.exit(other_pid, {:name_conflict, {key, other_value}, state.name, pid})
        state
      end)

    :ets.insert(state.keys_ets_table, {key, member, {pid, value}})

    unless already_visible? do
      for listener <- state.listeners do
        send(listener, {:register, state.name, key, pid, value})
      end
    end

    state
  end

  defp add_key_to_pids_table(state, pid, key) do
    case :ets.lookup(state.pids_ets_table, pid) do
      [] ->
        :ets.insert(state.pids_ets_table, {pid, [key]})

      [{^pid, keys}] ->
        :ets.insert(state.pids_ets_table, {pid, Enum.uniq([key | keys])})
    end
  end

  defp remove_key_from_pids_table(state, pid, key) do
    case :ets.lookup(state.pids_ets_table, pid) do
      [] ->
        :ok

      [{^pid, keys}] ->
        case List.delete(keys, key) do
          [] ->
            :ets.match_delete(state.pids_ets_table, {pid, :_})

          new_keys ->
            :ets.insert(state.pids_ets_table, {pid, new_keys})
        end
    end
  end

  defp link_local_pid(pid) when node(pid) == node() do
    Process.link(pid)
  end

  defp link_local_pid(_pid), do: nil

  def handle_call({:set_members, members}, _from, state) do
    new_members = MapSet.new(member_names(members))

    removed_members = MapSet.difference(state.members, new_members)
    added_members = MapSet.difference(new_members, state.members)

    DeltaCrdt.drop(
      crdt_name(state.name),
      Enum.map(removed_members, fn removed_member -> {:member, removed_member} end),
      :infinity
    )

    DeltaCrdt.merge(
      crdt_name(state.name),
      Map.new(added_members, fn added_member -> {{:member, added_member}, 1} end),
      :infinity
    )

    neighbours =
      MapSet.delete(new_members, fully_qualified_name(state.name))
      |> crdt_names()

    send(crdt_name(state.name), {:set_neighbours, neighbours})

    {:reply, :ok, %{state | members: new_members}}
  end

  def handle_call({:register, key, value, pid}, _from, state) do
    Process.link(pid)
    state = %{state | local_registrations: Map.put(state.local_registrations, key, {pid, value})}

    Horde.RegistryCrdt.put_registration(
      crdt_name(state.name),
      key,
      {fully_qualified_name(state.name), pid, value}
    )

    {:reply, {:ok, self()}, reconcile_registration(state, key)}
  end

  def handle_call({:update_value, key, pid, value}, _from, state) do
    state = %{state | local_registrations: Map.put(state.local_registrations, key, {pid, value})}

    Horde.RegistryCrdt.put_registration(
      crdt_name(state.name),
      key,
      {fully_qualified_name(state.name), pid, value}
    )

    :ets.insert(state.keys_ets_table, {key, fully_qualified_name(state.name), {pid, value}})

    {:reply, :ok, state}
  end

  def handle_call({:unregister, key, pid}, _from, state) do
    state = retire_registration(state, key, pid)
    Horde.RegistryCrdt.drop_owned(crdt_name(state.name), [key], pid)

    unregister_local(state, key, pid)

    {:reply, :ok, state}
  end

  def handle_call({:delete_meta, key}, _from, state) do
    DeltaCrdt.delete(crdt_name(state.name), {:registry, key}, :infinity)

    :ets.delete(state.name, key)

    {:reply, :ok, state}
  end

  def handle_call({:put_meta, key, value}, _from, state) do
    put_meta(state, key, value)

    {:reply, :ok, state}
  end

  def handle_call(:members, _from, state) do
    {:reply, MapSet.to_list(state.members), state}
  end

  defp retire_registration(state, key, pid) do
    case Map.get(state.local_registrations, key) do
      {^pid, _value} -> %{state | local_registrations: Map.delete(state.local_registrations, key)}
      _ -> state
    end
  end

  defp restore_local_registrations(state) do
    restore_local_registrations(state, Map.keys(state.local_registrations))
  end

  defp restore_local_registrations(state, keys) do
    Enum.reduce(keys, state, fn key, state ->
      case Map.get(state.local_registrations, key) do
        {pid, value} when node(pid) == node() ->
          if Process.alive?(pid) do
            Horde.RegistryCrdt.restore_registration(
              crdt_name(state.name),
              key,
              {fully_qualified_name(state.name), pid, value}
            )

            case DeltaCrdt.get(crdt_name(state.name), {:key, key}) do
              {_member, other_pid, _value} when other_pid != pid ->
                reconcile_registration(state, key)

              _ ->
                state
            end
          else
            retire_registration(state, key, pid)
          end

        _ ->
          state
      end
    end)
  end

  defp unregister_local(state, key) do
    case :ets.lookup(state.keys_ets_table, key) do
      [] ->
        nil

      [{key, _member, {pid, _val}}] ->
        remove_key_from_pids_table(state, pid, key)

        for listener <- state.listeners do
          send(listener, {:unregister, state.name, key, pid})
        end
    end

    :ets.match_delete(state.keys_ets_table, {key, :_, :_})
  end

  defp unregister_local(state, key, pid) do
    remove_key_from_pids_table(state, pid, key)

    for listener <- state.listeners do
      send(listener, {:unregister, state.name, key, pid})
    end

    :ets.match_delete(state.keys_ets_table, {key, :_, {pid, :_}})
  end

  defp member_names(names) do
    Enum.map(names, fn
      {name, node} -> {name, node}
      name when is_atom(name) -> {name, node()}
    end)
  end

  defp crdt_names(names) do
    Enum.map(names, fn {name, node} -> {crdt_name(name), node} end)
  end

  defp crdt_name(name), do: :"#{name}.Crdt"

  defp fully_qualified_name({name, node}) when is_atom(name) and is_atom(node), do: {name, node}
  defp fully_qualified_name(name) when is_atom(name), do: {name, node()}

  defp put_meta(state, key, value) do
    DeltaCrdt.put(crdt_name(state.name), {:registry, key}, value, :infinity)

    :ets.insert(state.registry_ets_table, {key, value})
  end
end
