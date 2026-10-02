defmodule Horde.PartialConnectivity.Distribution do
  @behaviour Horde.DistributionStrategy

  def choose_node(
        %{start: {Horde.PartialConnectivity.Worker, :start_link, [{:on_node, node, _}]}} = spec,
        members
      ) do
    case Enum.find(members, &match?(%{name: {_, ^node}, status: :alive}, &1)) do
      nil -> Horde.UniformDistribution.choose_node(spec, members)
      member -> {:ok, member}
    end
  end

  def choose_node(spec, members), do: Horde.UniformDistribution.choose_node(spec, members)
  defdelegate has_quorum?(members), to: Horde.UniformDistribution
end

defmodule Horde.PartialConnectivity.Worker do
  use GenServer

  def start(name) do
    Horde.DynamicSupervisor.start_child(TestSup, %{
      id: name,
      start: {__MODULE__, :start_link, [name]},
      restart: :transient
    })
  end

  def start_link(name),
    do: GenServer.start_link(__MODULE__, name, name: {:via, Horde.Registry, {TestReg, name}})

  def init(name) do
    Process.flag(:trap_exit, true)
    Agent.update(Horde.PartialConnectivity.Starts, &(&1 + 1))
    {:ok, name}
  end

  def handle_info({:EXIT, _, {:name_conflict, _, _, _}}, name), do: {:stop, :normal, name}
end

defmodule Horde.PartialConnectivity.Support do
  def start(membership) do
    children = [
      %{
        id: Horde.PartialConnectivity.Starts,
        start: {Agent, :start_link, [fn -> 0 end, [name: Horde.PartialConnectivity.Starts]]}
      },
      {Horde.Registry,
       name: TestReg, keys: :unique, members: membership, delta_crdt_options: [sync_interval: 20]},
      {Horde.DynamicSupervisor,
       name: TestSup,
       strategy: :one_for_one,
       distribution_strategy: Horde.PartialConnectivity.Distribution,
       members: membership,
       delta_crdt_options: [sync_interval: 20]}
    ]

    {:ok, pid} = Supervisor.start_link(children, strategy: :one_for_one)
    Process.unlink(pid)
    :ok
  end

  def snapshot(names) do
    state = :sys.get_state(TestSup)

    %{
      children:
        Horde.ProcessesSupervisor.which_children(TestSup.ProcessesSupervisor) |> Enum.sort(),
      records: :ets.tab2list(state.processes_by_id),
      registrations: Enum.map(names, &{&1, Horde.Registry.lookup(TestReg, &1)}),
      starts: Agent.get(Horde.PartialConnectivity.Starts, & &1),
      nodes: Node.list(),
      events: Horde.TestCluster.NodeEvents.events()
    }
  end
end
