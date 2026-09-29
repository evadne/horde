defmodule Worker do
  require Logger

  defstruct name: "",
            state: ""

  def state(name) do
    GenServer.call(via_tuple(name), :state)
  end

  def set_state(pid, state) when is_pid(pid) do
    GenServer.call(pid, {:set_state, state})
  end

  def set_state(name, state) do
    GenServer.call(via_tuple(name), {:set_state, state})
  end

  def start(name) do
    Horde.DynamicSupervisor.start_child(
      TestSup,
      child_spec(name: name)
    )
  end

  def via_tuple(name) do
    {:via, Horde.Registry, {TestReg, name}}
  end

  def start_link(name) do
    GenServer.start_link(__MODULE__, name, name: via_tuple(name))
    |> case do
      {:error, {:already_started, _pid}} -> :ignore
      result -> result
    end
  end

  def init(name) do
    Process.flag(:trap_exit, true)
    Logger.info("Starting worker on #{inspect(Node.self())}")
    {:ok, %__MODULE__{name: name}, {:continue, :started}}
  end

  def handle_continue(:started, state) do
    Logger.info("Started worker on #{inspect(Node.self())}")
    {:noreply, state}
  end

  def handle_call({:set_state, value}, _from, state) do
    {:reply, :ok, %{state | state: value}}
  end

  def handle_call(:state, _from, state) do
    {:reply, {:ok, state.state}, state}
  end

  # Follow Horde's documented conflict protocol: a duplicate retires normally
  # instead of triggering a restart storm when partitions merge.
  def handle_info({:EXIT, _from, {:name_conflict, _, _, _}}, state) do
    {:stop, :normal, state}
  end

  defp child_spec(name: name) do
    %{
      id: String.to_atom("#{__MODULE__}_#{name}"),
      start: {__MODULE__, :start_link, [name]},
      restart: :transient,
      shutdown: 10_000
    }
  end
end
