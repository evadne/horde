defmodule Horde.TestCluster do
  @moduledoc false
  import ExUnit.Assertions

  # The control channel must survive loss of Erlang distribution. In particular,
  # neither observations nor cleanup may reconnect a partition under test.
  def start_nodes(prefix, count, options \\ []) do
    for index <- 1..count do
      name = :"#{prefix}-#{System.unique_integer([:positive])}-#{index}"

      {:ok, controller, node} =
        :peer.start(%{
          name: name,
          host: ~c"127.0.0.1",
          longnames: true,
          connection: :standard_io,
          args: [~c"+S", ~c"2", ~c"-setcookie", Atom.to_charlist(Node.get_cookie())]
        })

      peer = %{controller: controller, node: node}
      ExUnit.Callbacks.on_exit(fn -> stop(peer) end)
      true = call(peer, :application, :get_env, [:kernel, :prevent_overlapping_partitions, true])
      :ok = call(peer, :code, :add_paths, [:code.get_path()])

      for app <- Keyword.get(options, :applications, [:horde, :test_app]) do
        {:ok, _} = call(peer, Application, :ensure_all_started, [app])
      end

      true = call(peer, Node, :connect, [node()])
      peer
    end
  end

  def nodes(peers), do: Enum.map(peers, & &1.node)

  def await_supervised(peers, pids) do
    await("replication of the supervised processes before changing topology", fn ->
      Enum.all?(peers, fn peer ->
        call(peer, __MODULE__, :supervised_pids, [TestSup]) |> Enum.sort() == Enum.sort(pids)
      end)
    end)
  end

  def supervised_pids(supervisor) do
    supervisor
    |> :sys.get_state()
    |> Map.fetch!(:processes_by_id)
    |> :ets.tab2list()
    |> Enum.map(fn {_id, {_member, _specification, pid}} -> pid end)
  end

  def recovery_snapshot do
    state = :sys.get_state(TestSup)

    %{
      connected: Node.list(),
      members: state.members_info,
      supervised: :ets.tab2list(state.processes_by_id),
      children: Horde.ProcessesSupervisor.which_children(TestSup.ProcessesSupervisor),
      registry: DeltaCrdt.to_map(TestReg.Crdt)
    }
  end

  def call(peer, module, function, arguments) do
    :peer.call(peer.controller, module, function, arguments, 5_000)
  end

  def stop(peer) do
    try do
      :peer.stop(peer.controller)
    catch
      :exit, :noproc -> :ok
      :exit, {:noproc, _} -> :ok
      :exit, {:normal, _} -> :ok
    end
  end

  def partition(groups) do
    all_nodes = [node() | nodes(List.flatten(groups))]

    # Distinct cookies prevent application traffic and global from healing the
    # cut. Explicit per-node cookies also replace any cached authentication.
    for group <- groups do
      cookie = :"partition_#{System.unique_integer([:positive])}"

      for peer <- group, other <- all_nodes do
        true = call(peer, :erlang, :set_cookie, [other, cookie])
      end
    end

    for group <- groups, peer <- group do
      for other <- call(peer, Node, :list, []), other not in nodes(group) do
        call(peer, Node, :disconnect, [other])
      end
    end

    # global can discard additional connections while a split forms. Restore
    # each intended, fully connected component without disabling that protection.
    for group <- groups, do: connect(group, nodes(group))

    assert_partition(groups)
  end

  def assert_partition(groups) do
    await("the requested partition topology", fn ->
      Enum.all?(groups, fn group ->
        Enum.all?(group, fn peer ->
          Enum.sort(call(peer, Node, :list, [])) == Enum.sort(nodes(group) -- [peer.node])
        end)
      end)
    end)
  end

  def heal(peers) do
    for peer <- peers, other <- [node() | nodes(peers)] do
      true = call(peer, :erlang, :set_cookie, [other, Node.get_cookie()])
    end

    connect(peers, [node() | nodes(peers)])
  end

  defp connect(peers, expected) do
    # A connection attempt can race with an outstanding global disconnect or
    # authentication handshake. Retry the requested network operation within
    # the deadline, then synchronise global before accepting the topology.
    await("the connected component #{inspect(expected)}", fn ->
      for peer <- peers, other <- expected, other != peer.node do
        call(peer, Node, :connect, [other])
      end

      for peer <- peers, do: call(peer, :global, :sync, [])

      Enum.all?(peers, fn peer ->
        Enum.sort(call(peer, Node, :list, [])) == Enum.sort(expected -- [peer.node])
      end)
    end)
  end

  def await(description, check, timeout \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    await_until(description, check, deadline)
  end

  defp await_until(description, check, deadline) do
    unless check.() do
      if System.monotonic_time(:millisecond) >= deadline do
        flunk("Timed out waiting for #{description}")
      end

      Process.sleep(10)
      await_until(description, check, deadline)
    end
  end
end
