defmodule Horde.TestCluster.NodeEvents do
  @moduledoc false
  use GenServer

  def start, do: GenServer.start(__MODULE__, nil, name: __MODULE__)
  def events, do: GenServer.call(__MODULE__, :events)

  @impl true
  def init(nil) do
    :ok = :net_kernel.monitor_nodes(true, [:nodedown_reason, {:node_type, :visible}])
    {:ok, []}
  end

  @impl true
  def handle_info({event, node, info}, events) when event in [:nodeup, :nodedown] do
    {:noreply, [{System.system_time(:microsecond), event, node, info} | events]}
  end

  @impl true
  def handle_call(:events, _from, events), do: {:reply, Enum.reverse(events), events}
end
