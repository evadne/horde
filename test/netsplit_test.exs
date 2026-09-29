defmodule NetSplitTest do
  use ExUnit.Case
  alias Horde.TestCluster, as: Cluster

  test "two live components retain their workers and converge after healing" do
    [first, second, third, fourth] = peers = Cluster.start_nodes("netsplit", 4)
    groups = [[first, second], [third, fourth]]
    nodes = Cluster.nodes(peers)

    for peer <- peers, name <- [TestSup, TestReg] do
      :ok = Cluster.call(peer, Horde.Cluster, :set_members, [name, Enum.map(nodes, &{name, &1})])
    end

    Cluster.heal(peers)
    names = Enum.map(1..20, &"worker-#{&1}")
    for name <- names, do: assert({:ok, _} = Cluster.call(first, Worker, :start, [name]))
    await_names(peers, names)
    Cluster.await_supervised(peers, Enum.map(names, &lookup(first, &1)))
    identities = Enum.map(peers, &Cluster.call(&1, Process, :whereis, [TestApp.Supervisor]))

    Cluster.partition(groups)

    try do
      for group <- groups do
        Cluster.await("one registered worker per name in the component", fn ->
          Enum.all?(names, fn name ->
            pids = Enum.map(group, &lookup(&1, name))

            case Enum.uniq(pids) do
              [pid] when is_pid(pid) ->
                node(pid) in Cluster.nodes(group) and
                  Cluster.call(Enum.find(group, &(&1.node == node(pid))), Process, :alive?, [pid])

              _ ->
                false
            end
          end)
        end)
      end
    rescue
      error in ExUnit.AssertionError ->
        for peer <- peers do
          IO.inspect(Cluster.call(peer, Cluster, :recovery_snapshot, []),
            label: "Partition recovery #{peer.node}",
            limit: :infinity
          )
        end

        reraise error, __STACKTRACE__
    end

    Cluster.assert_partition(groups)

    assert identities ==
             Enum.map(peers, &Cluster.call(&1, Process, :whereis, [TestApp.Supervisor]))

    Cluster.heal(peers)
    await_names(peers, names)

    Cluster.await("exactly one supervised worker per name after healing", fn ->
      children =
        Enum.flat_map(peers, fn peer ->
          Cluster.call(peer, Horde.ProcessesSupervisor, :which_children, [
            TestSup.ProcessesSupervisor
          ])
        end)

      registered = Enum.map(names, &lookup(first, &1))
      Enum.sort(Enum.map(children, &elem(&1, 1))) == Enum.sort(registered)
    end)

    assert identities ==
             Enum.map(peers, &Cluster.call(&1, Process, :whereis, [TestApp.Supervisor]))
  end

  defp lookup(peer, name),
    do: Cluster.call(peer, Horde.Registry, :whereis_name, [{TestReg, name}])

  defp await_names(peers, names) do
    Cluster.await("agreement on all registered workers", fn ->
      Enum.all?(names, fn name ->
        case Enum.uniq(Enum.map(peers, &lookup(&1, name))) do
          [pid] when is_pid(pid) -> true
          _ -> false
        end
      end)
    end)
  end
end
