defmodule Horde.ProxyOperation do
  @moduledoc false

  # A single relay follows an operation through successive owners. In particular,
  # an infinite proxy TTL must not create an unbounded chain of waiting processes.
  def forward(destination, message, {relay, {:horde_proxy, _}} = reply_to, ttl) do
    send(relay, {:forward, destination, message, reply_to, ttl})
    :ok
  end

  def forward(destination, message, reply_to, ttl) do
    owner = self()

    spawn(fn ->
      owner_ref = Process.monitor(owner)
      caller_ref = Process.monitor(elem(reply_to, 0))
      tag = {:horde_proxy, make_ref()}
      relay_from = {self(), tag}
      destination_ref = deliver(destination, message, relay_from, ttl)
      await_reply(reply_to, tag, owner_ref, caller_ref, destination_ref)
    end)

    :ok
  end

  defp deliver(destination, message, reply_to, ttl) do
    ref = Process.monitor(destination)

    case ttl do
      :infinity -> send(destination, {:proxy_operation, message, reply_to})
      ttl -> send(destination, {:proxy_operation, message, reply_to, ttl})
    end

    ref
  end

  defp await_reply(reply_to, tag, owner_ref, caller_ref, destination_ref) do
    receive do
      {^tag, reply} ->
        GenServer.reply(reply_to, reply)

      {:forward, destination, message, {_relay, ^tag} = relay_from, ttl} ->
        Process.demonitor(destination_ref, [:flush])
        next_ref = deliver(destination, message, relay_from, ttl)
        await_reply(reply_to, tag, owner_ref, caller_ref, next_ref)

      {:DOWN, ^destination_ref, :process, destination, reason} ->
        # The operation may have completed before connectivity was lost. Do not
        # retry it: callers must reconcile any uncertain result themselves.
        GenServer.reply(reply_to, {:error, {:proxy_target_down, destination, reason}})

      {:DOWN, ^owner_ref, :process, _owner, _reason} ->
        :ok

      {:DOWN, ^caller_ref, :process, _caller, _reason} ->
        :ok
    end
  end
end
