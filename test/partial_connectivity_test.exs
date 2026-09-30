defmodule Horde.PartialConnectivityTest do
  use ExUnit.Case, async: false

  alias Horde.PartialConnectivity.{Support, Worker}
  alias Horde.TestCluster

  @moduletag timeout: 90_000
  @cookie :horde_partial_connectivity

  for membership <- [:static, :auto] do
    @membership membership
    test "#{membership} membership restores exact ownership after a partial network failure" do
      peers = start_peers(@membership)
      [a, b, c] = peers
      connect(peers)

      if @membership == :static do
        for peer <- peers, name <- [TestSup, TestReg] do
          :ok = call(peer, Horde.Cluster, :set_members, [name, Enum.map(peers, &{name, &1.node})])
        end
      end

      TestCluster.await("all members join", fn ->
        Enum.all?(peers, fn peer ->
          Enum.all?([TestSup, TestReg], fn name ->
            length(call(peer, Horde.Cluster, :members, [name])) == 3
          end)
        end)
      end)

      names = Enum.map(1..12, &"partial-worker-#{&1}")
      for name <- names, do: assert({:ok, _} = call(a, Worker, :start, [name]))
      await_agreement(peers, names)

      true = call(a, :erlang, :set_cookie, [c.node, :blocked_from_a])
      true = call(c, :erlang, :set_cookie, [a.node, :blocked_from_c])
      true = call(a, Node, :disconnect, [c.node])

      # Allow membership and conflict resolution to settle, then observe several
      # recovery intervals. A stable fault must not cause repeated replacement.
      before = await_quiet(peers)
      Process.sleep(1_500)
      after_fault = snapshots(peers, [])
      assert Enum.map(before, & &1.starts) == Enum.map(after_fault, & &1.starts)
      assert Enum.map(before, & &1.children) == Enum.map(after_fault, & &1.children)

      for {peer, snapshot} <- Enum.zip(peers, after_fault) do
        expected = if peer.node == b.node, do: [a.node, c.node], else: [b.node]
        assert Enum.sort(snapshot.nodes) == Enum.sort(expected)

        assert Enum.all?(snapshot.events, fn
                 {_, :nodedown, down, _} ->
                   {peer.node, down} in [{a.node, c.node}, {c.node, a.node}]

                 _ ->
                   true
               end)
      end

      true = call(a, :erlang, :set_cookie, [c.node, @cookie])
      true = call(c, :erlang, :set_cookie, [a.node, @cookie])
      connect(peers)
      await_agreement(peers, names)

      fresh = "fresh-after-healing"
      assert {:ok, fresh_pid} = call(b, Worker, :start, [fresh])
      names = names ++ [fresh]
      await_agreement(peers, names)

      owner = Enum.find(peers, &(&1.node == node(fresh_pid)))
      stop(owner)
      survivors = Enum.reject(peers, &(&1 == owner))
      await_agreement(survivors, names)

      [{^fresh, [{replacement, nil}]}] =
        call(hd(survivors), Support, :snapshot, [[fresh]]).registrations

      assert replacement != fresh_pid
    end
  end

  defp start_peers(membership) do
    for index <- 1..3 do
      {:ok, controller, node} =
        :peer.start(%{
          name: :"partial-#{System.unique_integer([:positive])}-#{index}",
          host: ~c"127.0.0.1",
          longnames: true,
          connection: :standard_io,
          args: [
            ~c"+S",
            ~c"2",
            ~c"-setcookie",
            Atom.to_charlist(@cookie),
            ~c"-kernel",
            ~c"prevent_overlapping_partitions",
            ~c"false"
          ]
        })

      peer = %{controller: controller, node: node}
      on_exit(fn -> stop(peer) end)
      false = call(peer, :application, :get_env, [:kernel, :prevent_overlapping_partitions, true])
      :ok = call(peer, :code, :add_paths, [:code.get_path()])
      {:ok, _} = call(peer, Application, :ensure_all_started, [:horde])
      :ok = call(peer, Application, :stop, [:test_app])
      {:ok, _} = call(peer, Horde.TestCluster.NodeEvents, :start, [])
      :ok = call(peer, Support, :start, [if(membership == :auto, do: :auto, else: [])])
      peer
    end
  end

  defp connect(peers) do
    for peer <- peers, other <- peers, peer != other, do: call(peer, Node, :connect, [other.node])

    TestCluster.await("full mesh", fn ->
      Enum.all?(peers, fn peer ->
        Enum.sort(call(peer, Node, :list, [])) == Enum.sort(Enum.map(peers -- [peer], & &1.node))
      end)
    end)
  end

  defp snapshots(peers, names), do: Enum.map(peers, &call(&1, Support, :snapshot, [names]))

  defp await_quiet(peers) do
    now = System.monotonic_time(:millisecond)
    await_quiet(peers, snapshots(peers, []), now, now + 10_000)
  end

  defp await_quiet(peers, previous, unchanged_since, deadline) do
    Process.sleep(100)
    current = snapshots(peers, [])
    now = System.monotonic_time(:millisecond)
    fingerprint = fn snapshots -> Enum.map(snapshots, &{&1.starts, &1.children}) end

    unchanged_since =
      if fingerprint.(current) == fingerprint.(previous), do: unchanged_since, else: now

    cond do
      now - unchanged_since >= 1_500 ->
        current

      now >= deadline ->
        flunk("worker starts and actual children never stabilised during the fault")

      true ->
        await_quiet(peers, current, unchanged_since, deadline)
    end
  end

  defp await_agreement(peers, names) do
    TestCluster.await(
      "registry, actual workers and every replicated child specification agree",
      fn ->
        snapshots = snapshots(peers, names)
        registrations = Enum.map(snapshots, & &1.registrations) |> Enum.uniq()

        case registrations do
          [entries] ->
            if Enum.all?(entries, fn {_, value} -> match?([{pid, _}] when is_pid(pid), value) end) do
              expected =
                Enum.map(entries, fn {name, [{pid, _}]} -> {name, pid} end) |> Enum.sort()

              pids = Enum.map(expected, &elem(&1, 1)) |> Enum.sort()

              actual =
                Enum.flat_map(snapshots, & &1.children) |> Enum.map(&elem(&1, 1)) |> Enum.sort()

              record_sets = Enum.map(snapshots, &Enum.sort(&1.records)) |> Enum.uniq()

              actual == pids and length(record_sets) == 1 and
                Enum.all?(snapshots, fn snapshot ->
                  records =
                    Enum.map(snapshot.records, fn {_id, {member, spec, pid}} ->
                      {Worker, :start_link, [name]} = spec.start
                      {name, member, spec.start, pid}
                    end)
                    |> Enum.sort()

                  records ==
                    Enum.map(expected, fn {name, pid} ->
                      {name, {TestSup, node(pid)}, {Worker, :start_link, [name]}, pid}
                    end)
                end)
            else
              false
            end

          _ ->
            false
        end
      end,
      15_000
    )
  rescue
    error in ExUnit.AssertionError ->
      for {peer, snapshot} <- Enum.zip(peers, snapshots(peers, names)) do
        IO.inspect(snapshot,
          label: "Ownership agreement failure on #{peer.node}",
          limit: :infinity
        )
      end

      reraise error, __STACKTRACE__
  end

  defp call(peer, module, function, arguments),
    do: :peer.call(peer.controller, module, function, arguments, 5_000)

  defp stop(peer) do
    if Process.alive?(peer.controller), do: :peer.stop(peer.controller)
  catch
    :exit, _ -> :ok
  end
end
