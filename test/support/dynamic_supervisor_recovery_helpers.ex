defmodule Horde.RecoveryTest.FirstMemberDistribution do
  @behaviour Horde.DistributionStrategy

  def choose_node(_spec, members) do
    case members |> Enum.filter(&(&1.status == :alive)) |> Enum.sort_by(& &1.name) do
      [member | _] -> {:ok, member}
      [] -> {:error, :no_alive_nodes}
    end
  end

  def has_quorum?(_members), do: true
end

defmodule Horde.RecoveryTest.NamedWorker do
  use GenServer

  def start_link({name, observer, gate}) do
    send(observer, {:start_attempt, name})

    case Agent.get(gate, & &1) do
      :ignore -> :ignore
      :run -> GenServer.start_link(__MODULE__, {name, observer}, name: name)
    end
  end

  def init({name, observer}) do
    send(observer, {:started, name, self()})
    {:ok, nil}
  end
end

defmodule Horde.RecoveryTest.GuardedWorker do
  use GenServer

  def start_link({name, initial_owner, bridge}) do
    # Model a failed recovery while another observer can still reach the old
    # owner. Once it actually dies, the same local start is allowed to succeed.
    if node() != initial_owner and
         :erpc.call(bridge, Horde.Registry, :lookup, [TestReg, name], 1_000) != [] do
      {:error, :owner_still_visible_to_bridge}
    else
      GenServer.start_link(__MODULE__, nil, name: {:via, Horde.Registry, {TestReg, name}})
    end
  end

  def init(_) do
    Process.flag(:trap_exit, true)
    {:ok, nil}
  end

  def handle_info({:EXIT, _, {:name_conflict, _, _, _}}, state), do: {:stop, :normal, state}
end

defmodule Horde.RecoveryTest.Support do
  def start do
    children = [
      {Horde.DynamicSupervisor,
       name: TestSup,
       strategy: :one_for_one,
       distribution_strategy: Horde.RecoveryTest.FirstMemberDistribution,
       delta_crdt_options: [sync_interval: 20]},
      {Horde.Registry, name: TestReg, keys: :unique, delta_crdt_options: [sync_interval: 20]}
    ]

    {:ok, pid} = Supervisor.start_link(children, strategy: :one_for_one)
    Process.unlink(pid)
    :ok
  end

  def snapshot(name) do
    state = :sys.get_state(name)

    %{
      unavailable: state.unavailable_members,
      pending: state.pending_recoveries,
      members: state.members_info,
      observations: state.member_observations,
      local: state.local_processes,
      records: :ets.tab2list(state.processes_by_id),
      replicated: DeltaCrdt.to_map(:"#{name}.Crdt")
    }
  end

  def registry_neighbours(neighbours) do
    send(TestReg.Crdt, {:set_neighbours, neighbours})
    :sys.get_state(TestReg.Crdt).neighbours
  end
end

defmodule Horde.RecoveryTest.UnnamedWorker do
  use GenServer
  def start_link(_), do: GenServer.start_link(__MODULE__, nil)
  def init(_), do: {:ok, nil}
end
