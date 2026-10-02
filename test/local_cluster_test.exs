defmodule TestClusterTest do
  use ExUnit.Case
  alias Horde.TestCluster, as: Cluster

  test "partition observations preserve live nodes without reconnecting distribution" do
    [first, second] =
      peers = Cluster.start_nodes("control", 2, applications: [], connect_manager: false)

    Cluster.heal(peers)

    sentinels =
      Enum.map(peers, &Cluster.call(&1, :erlang, :spawn, [Process, :sleep, [:infinity]]))

    for peer <- peers do
      assert Cluster.call(peer, Application, :get_env, [
               :kernel,
               :prevent_overlapping_partitions,
               true
             ])
    end

    Cluster.partition([[first], [second]])

    for {peer, pid} <- Enum.zip(peers, sentinels) do
      assert Cluster.call(peer, Process, :alive?, [pid])
      assert Cluster.call(peer, Node, :list, []) == []
    end

    # Even attempted distribution traffic must not heal the partition. All
    # subsequent observations continue over the independent control channel.
    assert :pang == Cluster.call(first, Node, :ping, [second.node])
    assert :pang == Cluster.call(second, Node, :ping, [first.node])
    Cluster.assert_partition([[first], [second]])
    Cluster.heal(peers)

    for {peer, pid} <- Enum.zip(peers, sentinels) do
      assert Cluster.call(peer, Process, :alive?, [pid])

      assert Enum.sort(Cluster.call(peer, Node, :list, [])) ==
               Enum.sort(Cluster.nodes(peers) -- [peer.node])

      events = Cluster.call(peer, Horde.TestCluster.NodeEvents, :events, [])
      refute Enum.any?(events, fn {_time, _event, remote, _info} -> remote == node() end)
      assert Enum.any?(events, fn {_time, event, _remote, _info} -> event == :nodedown end)
    end
  end
end
