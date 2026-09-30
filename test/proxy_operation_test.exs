defmodule Horde.ProxyOperationTest do
  use ExUnit.Case, async: true

  defp destination do
    parent = self()

    spawn(fn ->
      receive do
        message ->
          send(parent, {:delivered, self(), message})

          receive do
            :stop -> :ok
          end
      end
    end)
  end

  defp begin_operation(destination) do
    reply_tag = make_ref()
    :ok = Horde.ProxyOperation.forward(destination, :request, {self(), reply_tag}, :infinity)
    assert_receive {:delivered, ^destination, {:proxy_operation, :request, relay_from}}
    relay = elem(relay_from, 0)
    monitor = monitor_relay(relay)
    {reply_tag, relay_from, monitor}
  end

  defp monitor_relay(relay) do
    monitor = Process.monitor(relay)
    # Monitor installation may complete asynchronously on current OTP. Confirm
    # it before another process can make the relay exit and yield :noproc.
    assert {:monitored_by, observers} = Process.info(relay, :monitored_by)
    assert self() in observers
    monitor
  end

  test "returns an uncertain error when the destination dies before replying" do
    destination = destination()
    {reply_tag, {relay, _}, monitor} = begin_operation(destination)
    Process.exit(destination, :kill)

    assert_receive {^reply_tag, {:error, {:proxy_target_down, ^destination, :killed}}}
    assert_receive {:DOWN, ^monitor, :process, ^relay, :normal}
  end

  test "delivers a successful reply once and releases all monitors" do
    destination = destination()
    {reply_tag, {relay, _} = relay_from, monitor} = begin_operation(destination)
    GenServer.reply(relay_from, {:ok, :started})

    assert_receive {^reply_tag, {:ok, :started}}
    assert_receive {:DOWN, ^monitor, :process, ^relay, :normal}
    Process.exit(destination, :kill)
    refute_receive {^reply_tag, _}
  end

  test "ignores a late reply after destination failure" do
    destination = destination()
    {reply_tag, {relay, _} = relay_from, monitor} = begin_operation(destination)
    Process.exit(destination, :kill)

    assert_receive {^reply_tag, {:error, {:proxy_target_down, ^destination, :killed}}}
    assert_receive {:DOWN, ^monitor, :process, ^relay, :normal}
    GenServer.reply(relay_from, {:ok, :late})
    refute_receive {^reply_tag, _}
  end

  test "follows the current destination without nesting relays" do
    first = destination()
    second = destination()
    {reply_tag, {relay, _} = relay_from, monitor} = begin_operation(first)
    :ok = Horde.ProxyOperation.forward(second, :request, relay_from, 3)
    assert_receive {:delivered, ^second, {:proxy_operation, :request, ^relay_from, 3}}
    Process.exit(first, :kill)
    refute_receive {^reply_tag, _}
    Process.exit(second, :kill)

    assert_receive {^reply_tag, {:error, {:proxy_target_down, ^second, :killed}}}
    assert_receive {:DOWN, ^monitor, :process, ^relay, :normal}
  end

  test "forwards a final reply after multiple hops and preserves finite TTL" do
    destinations = [first, second, third] = for _ <- 1..3, do: destination()
    {reply_tag, {relay, _} = relay_from, monitor} = begin_operation(first)

    for {destination, ttl} <- [{second, 2}, {third, 1}] do
      :ok = Horde.ProxyOperation.forward(destination, :request, relay_from, ttl)
      assert_receive {:delivered, ^destination, {:proxy_operation, :request, ^relay_from, ^ttl}}
    end

    GenServer.reply(relay_from, {:ok, :started})
    assert_receive {^reply_tag, {:ok, :started}}
    assert_receive {:DOWN, ^monitor, :process, ^relay, :normal}
    Enum.each(destinations, &Process.exit(&1, :kill))
  end

  test "stops when its caller dies" do
    destination = destination()
    caller = spawn(fn -> Process.sleep(:infinity) end)
    tag = make_ref()
    :ok = Horde.ProxyOperation.forward(destination, :request, {caller, tag}, :infinity)
    assert_receive {:delivered, ^destination, {:proxy_operation, :request, {relay, _}}}
    monitor = monitor_relay(relay)
    Process.exit(caller, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^relay, :normal}
    Process.exit(destination, :kill)
  end

  test "stops when its originating supervisor dies" do
    destination = destination()
    parent = self()
    tag = make_ref()

    owner =
      spawn(fn ->
        Horde.ProxyOperation.forward(destination, :request, {parent, tag}, :infinity)
        Process.sleep(:infinity)
      end)

    assert_receive {:delivered, ^destination, {:proxy_operation, :request, {relay, _}}}
    monitor = monitor_relay(relay)
    Process.exit(owner, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^relay, :normal}
    Process.exit(destination, :kill)
  end
end
