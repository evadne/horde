defmodule Horde.ProxyTargetDownTest do
  use ExUnit.Case

  defmodule LastMemberDistribution do
    def has_quorum?(_members), do: true
    def choose_node(_child_spec, members), do: {:ok, Enum.max_by(members, & &1.name)}
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

  defp await(check, attempts \\ 100)
  defp await(_check, 0), do: flunk("condition did not become true")

  defp await(check, attempts) do
    unless check.() do
      Process.sleep(10)
      await(check, attempts - 1)
    end
  end
end
