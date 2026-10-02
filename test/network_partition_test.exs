defmodule NetworkPartitionTest do
  use ExUnit.Case
  alias Horde.TestCluster, as: Cluster

  setup do
    peers = Cluster.start_nodes("partition", 2, connect_manager: false)
    members = Cluster.nodes(peers)

    for peer <- peers do
      :ok =
        Cluster.call(peer, Horde.Cluster, :set_members, [
          TestReg,
          Enum.map(members, &{TestReg, &1})
        ])

      :ok =
        Cluster.call(peer, Horde.Cluster, :set_members, [
          TestSup,
          Enum.map(members, &{TestSup, &1})
        ])
    end

    Cluster.heal(peers)
    [peers: peers]
  end

  test "recovers after a partition while both nodes and the original worker survive", %{
    peers: [first, second] = peers
  } do
    {:ok, original} = Cluster.call(first, Worker, :start, ["partition-worker"])
    owner = Enum.find(peers, &(&1.node == node(original)))
    assert :ok = Cluster.call(owner, Worker, :set_state, [original, "preserved"])
    await_registered(peers, "partition-worker", original)
    Cluster.await_supervised(peers, [original])
    identities = Enum.map(peers, &Cluster.call(&1, Process, :whereis, [TestApp.Supervisor]))

    Cluster.partition([[first], [second]])
    assert Cluster.call(owner, Process, :alive?, [original])
    assert {:ok, "preserved"} = Cluster.call(owner, GenServer, :call, [original, :state])

    assert identities ==
             Enum.map(peers, &Cluster.call(&1, Process, :whereis, [TestApp.Supervisor]))

    Cluster.await("a local replacement in each partition", fn ->
      Enum.all?(peers, fn peer ->
        pid = lookup(peer, "partition-worker")
        is_pid(pid) and node(pid) == peer.node and Cluster.call(peer, Process, :alive?, [pid])
      end)
    end)

    partition_pids = Enum.map(peers, &lookup(&1, "partition-worker"))
    assert length(Enum.uniq(partition_pids)) == 2
    Cluster.assert_partition([[first], [second]])
    Cluster.heal(peers)

    Cluster.await("registry agreement and exactly one surviving worker after healing", fn ->
      pids = Enum.map(peers, &lookup(&1, "partition-worker"))

      alive =
        Enum.filter(partition_pids, fn pid ->
          peer = Enum.find(peers, &(&1.node == node(pid)))
          Cluster.call(peer, Process, :alive?, [pid])
        end)

      match?([pid] when is_pid(pid), Enum.uniq(pids)) and alive == Enum.uniq(pids)
    end)

    assert identities ==
             Enum.map(peers, &Cluster.call(&1, Process, :whereis, [TestApp.Supervisor]))
  end

  test "restarts the worker on the surviving node after its owner stops", %{
    peers: [first | _] = peers
  } do
    {:ok, original} = Cluster.call(first, Worker, :start, ["stopped-worker"])
    await_registered(peers, "stopped-worker", original)
    Cluster.await_supervised(peers, [original])
    owner = Enum.find(peers, &(&1.node == node(original)))
    survivor = Enum.find(peers, &(&1 != owner))
    Cluster.stop(owner)

    Cluster.await("a replacement on the surviving node", fn ->
      pid = lookup(survivor, "stopped-worker")

      is_pid(pid) and pid != original and node(pid) == survivor.node and
        Cluster.call(survivor, Process, :alive?, [pid])
    end)
  end

  defp lookup(peer, name),
    do: Cluster.call(peer, Horde.Registry, :whereis_name, [{TestReg, name}])

  defp await_registered(peers, name, pid) do
    Cluster.await("replication of the worker registration", fn ->
      Enum.all?(peers, &(lookup(&1, name) == pid))
    end)
  end
end
