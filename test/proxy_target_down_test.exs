defmodule Horde.ProxyTargetDownTest do
  use ExUnit.Case

  defmodule LastMemberDistribution do
    def has_quorum?(_members), do: true
    def choose_node(_child_spec, members), do: {:ok, Enum.max_by(members, & &1.name)}
  end

  defmodule DependentChild do
    use GenServer

    def start_link(argument), do: GenServer.start_link(__MODULE__, argument)

    def init({supervisor, observer}) do
      send(observer, {:initialising, self()})

      {:ok, dependency} =
        Horde.DynamicSupervisor.start_child(supervisor, {Agent, fn -> :ready end})

      {:ok, dependency}
    end
  end

  test "a failed remote dependency crashes the initialising child and releases its supervisor" do
    suffix = System.unique_integer([:positive])
    outer = :"outer_#{suffix}"
    first = :"dependency_a_#{suffix}"
    second = :"dependency_z_#{suffix}"
    [remote_node] = LocalCluster.start_nodes("nested-proxy-#{suffix}", 1)
    on_exit(fn -> LocalCluster.stop_nodes([remote_node]) end)

    for name <- [outer, first] do
      start_supervised!(%{
        id: name,
        start:
          {Horde.DynamicSupervisor, :start_link,
           [[name: name, strategy: :one_for_one, distribution_strategy: LastMemberDistribution]]},
        restart: :temporary
      })
    end

    remote_spec = %{
      id: second,
      start: {Horde.DynamicSupervisor, :start_link, [[name: second, strategy: :one_for_one]]},
      restart: :temporary
    }

    assert {:ok, _} =
             :erpc.call(remote_node, Supervisor, :start_child, [TestApp.Supervisor, remote_spec])

    remote_member = {second, remote_node}
    :ok = Horde.Cluster.set_members(first, [first, remote_member])
    await(fn -> map_size(:sys.get_state(first).name_to_supervisor_ref) == 2 end)
    selected_pid = :erpc.call(remote_node, Process, :whereis, [second])
    :ok = :sys.suspend(selected_pid)
    observer = self()

    task =
      Task.async(fn ->
        Horde.DynamicSupervisor.start_child(outer, {DependentChild, {first, observer}})
      end)

    assert_receive {:initialising, initialising_child}, 1_000
    child_monitor = Process.monitor(initialising_child)
    implementation = Process.whereis(outer)
    processes_supervisor = Process.whereis(:"#{outer}.ProcessesSupervisor")

    try do
      await(fn ->
        {:messages, messages} =
          :erpc.call(remote_node, Process, :info, [selected_pid, :messages])

        Enum.any?(messages, &match?({:proxy_operation, {:start_child, _}, _}, &1))
      end)

      true = :erpc.call(remote_node, Process, :exit, [selected_pid, :kill])
      failure = {:badmatch, {:error, {:proxy_target_down, remote_member, :killed}}}

      assert {:ok, {:error, {^failure, _stack}}} =
               Task.yield(task, 1_000) || Task.shutdown(task, :brutal_kill)

      assert_receive {:DOWN, ^child_monitor, :process, ^initialising_child, {^failure, _}}, 1_000

      # The failed initialisation has unwound. Its supervisors remain the same
      # processes, can reconcile membership and can start another child.
      assert Process.whereis(outer) == implementation
      assert Process.whereis(:"#{outer}.ProcessesSupervisor") == processes_supervisor
      assert :ok = Horde.Cluster.set_members(outer, [outer])
      assert %{local_supervised_process_count: 0} = GenServer.call(outer, :get_telemetry, 1_000)

      assert {:ok, sibling} =
               Horde.DynamicSupervisor.start_child(outer, {Agent, fn -> :healthy end})

      assert Agent.get(sibling, & &1) == :healthy
    after
      # Also release the deliberately blocked init when this regression is run
      # against unpatched Horde, so failed assertions do not hang test teardown.
      Process.exit(initialising_child, :kill)
    end
  end

  test "reports an uncertain outcome when the distribution connection is lost" do
    [remote_node] =
      LocalCluster.start_nodes("proxy-loss-#{System.unique_integer([:positive])}", 1)

    on_exit(fn ->
      LocalCluster.stop_nodes([remote_node])
    end)

    destination = :erpc.call(remote_node, :erlang, :spawn, [Process, :sleep, [:infinity]])
    tag = make_ref()
    :ok = Horde.ProxyOperation.forward(destination, :request, {self(), tag}, :infinity)

    await(fn ->
      {:messages, messages} = :erpc.call(remote_node, Process, :info, [destination, :messages])
      Enum.any?(messages, &match?({:proxy_operation, :request, _}, &1))
    end)

    assert true = Node.disconnect(remote_node)
    assert_receive {^tag, {:error, {:proxy_target_down, ^destination, :noconnection}}}, 1_000
    refute remote_node in Node.list()
  end

  test "a proxied start returns when its selected supervisor disappears" do
    {first, second} = start_pair()
    selected_pid = Process.whereis(second)
    :ok = :sys.suspend(selected_pid)

    task =
      Task.async(fn ->
        Horde.DynamicSupervisor.start_child(first, {Agent, fn -> :started end})
      end)

    await(fn ->
      {:messages, messages} = Process.info(selected_pid, :messages)
      Enum.any?(messages, &match?({:proxy_operation, {:start_child, _}, _}, &1))
    end)

    Process.exit(selected_pid, :kill)

    assert {:ok, {:error, {:proxy_target_down, {^second, _}, :killed}}} =
             Task.yield(task, 1_000) || Task.shutdown(task, :brutal_kill)

    assert is_map(GenServer.call(first, :get_telemetry, 1_000))
    assert :ok = Horde.Cluster.set_members(first, [first])
  end

  test "a proxied termination returns uncertainty when its owning supervisor disappears" do
    {first, second} = start_pair()
    {:ok, child} = Horde.DynamicSupervisor.start_child(first, {Agent, fn -> :started end})
    await(fn -> :ets.member(:sys.get_state(first).process_pid_to_id, child) end)
    selected_pid = Process.whereis(second)
    :ok = :sys.suspend(selected_pid)
    task = Task.async(fn -> Horde.DynamicSupervisor.terminate_child(first, child) end)

    await(fn ->
      {:messages, messages} = Process.info(selected_pid, :messages)
      Enum.any?(messages, &match?({:proxy_operation, {:terminate_child, ^child}, _}, &1))
    end)

    Process.exit(selected_pid, :kill)

    assert {:ok, {:error, {:proxy_target_down, {^second, _}, :killed}}} =
             Task.yield(task, 1_000) || Task.shutdown(task, :brutal_kill)

    assert is_map(GenServer.call(first, :get_telemetry, 1_000))
    assert :ok = Horde.Cluster.set_members(first, [first])
  end

  defp start_pair do
    suffix = System.unique_integer([:positive])
    first = :"proxy_a_#{suffix}"
    second = :"proxy_z_#{suffix}"

    for name <- [first, second] do
      start_supervised!(%{
        id: name,
        start:
          {Horde.DynamicSupervisor, :start_link,
           [
             [
               name: name,
               strategy: :one_for_one,
               distribution_strategy: LastMemberDistribution,
               delta_crdt_options: [sync_interval: 10]
             ]
           ]},
        restart: :temporary
      })
    end

    :ok = Horde.Cluster.set_members(first, [first, second])
    await(fn -> length(Horde.Cluster.members(second)) == 2 end)
    await(fn -> map_size(:sys.get_state(first).name_to_supervisor_ref) == 2 end)
    {first, second}
  end

  defp await(check, attempts \\ 100)
  defp await(_check, 0), do: flunk("condition did not become true")

  defp await(check, attempts) do
    unless check.() do
      Process.sleep(10)
      await(check, attempts - 1)
    end
  end
end
