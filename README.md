# Horde [![Hex pm](http://img.shields.io/hexpm/v/horde.svg?style=flat)](https://hex.pm/packages/horde) [![.github/workflows/ci.yml](https://github.com/derekkraan/horde/actions/workflows/ci.yml/badge.svg)](https://github.com/derekkraan/horde/actions/workflows/ci.yml) [![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/horde)


Distribute your application over multiple servers with Horde.

Horde is comprised of `Horde.DynamicSupervisor`, a distributed supervisor, and `Horde.Registry`, a distributed registry. Horde is built on top of [DeltaCrdt](https://github.com/derekkraan/delta_crdt_ex).

Read the [full documentation](https://hexdocs.pm/horde) on hexdocs.pm.

There is an [introductory blog post](https://moosecode.nl/blog/introducing_horde) and a [getting started guide](https://moosecode.nl/blog/getting_started_horde). You can also find me in the Elixir slack channel #horde.

Daniel Azuma gave [a great talk](https://www.youtube.com/watch?v=nLApFANtkHs) at ElixirConf US 2018 where he demonstrated Horde's Supervisor and Registry.

Since Horde is built on CRDTs, it is eventually (as opposed to immediately) consistent, although it does sync its state with its neighbours rather aggressively. Cluster membership in Horde is fully dynamic; nodes can be added and removed at any time and Horde will continue to operate as expected. `Horde.DynamicSupervisor` also uses a hash ring to limit any possible race conditions to times when cluster membership is changing. 

`Horde.Registry` and `Horde.DynamicSupervisor` are both designed to stay as close as possible to the API and behavior of their counterparts in Elixir’s standard library. For most scenarios, they can be used as drop-in replacements with minimal changes required.

Some differences do exist — such as the current lack of support for keys: :duplicate in Horde.Registry — but these divergences occur only when standard library behavior does not translate well to a system that is inherently distributed.

Our goal is to keep these differences to the absolute minimum necessary, while ensuring that Horde remains reliable, consistent, and optimized for distributed environments. See [documentation of Horde.DynamicSupervisor.start_link/1](https://hexdocs.pm/horde/Horde.DynamicSupervisor.html#start_link/1) for details.

## Running a single global process

If you simply need to run a single process as a singleton in your cluster, I would encourage you to look at [Highlander](https://github.com/derekkraan/highlander) or [HighlanderPG](https://hex.codecodeship.com/package/highlander_pg) instead, as one of these may fit your use case better.

## 1.0 release

Help us get to 1.0, please fill out our [very short survey](https://docs.google.com/forms/d/e/1FAIpQLSd0fGMuELJIKAiaR1XlvHKjpSo024cojktXjp4ASM7MSXTYfg/viewform?usp=sf_link) and report any issues you encounter when using Horde.

## Fault tolerance

If a node fails (or otherwise becomes unreachable) then Horde.DynamicSupervisor will redistribute processes among the remaining nodes.

You can choose what to do in the event of a network partition by specifying `:distribution_strategy` in the options for `Horde.DynamicSupervisor.start_link/2`. Setting this option to `Horde.UniformDistribution` (which is the default) distributes processes using a hash mechanism among all reachable nodes. In the event of a network partition, both sides of the partition will continue to operate. Setting it to `Horde.UniformQuorumDistribution` will operate in the same way, but will shut down if less than half of the cluster is reachable.

## CAP Theorem

Horde is eventually consistent, which means that Horde can guarantee availability and partition tolerancy. Horde cannot guarantee consistency. This means you may end up with duplicate processes in your cluster. Horde does aggressively synchronize between nodes (this is also tunable), but ultimately, depending on the tuning parameters you choose and the quality of the network, there are conditions under which it is possible to have duplicate processes in your cluster. Horde.Registry terminates duplicate processes as soon as they are discovered with a special exit code, so you'll always know when this is happening. See [this page in the docs](https://hexdocs.pm/horde/eventual_consistency.html#horde-registry-merge-conflict) for more details.

_NOTE: Since Horde 0.6.0, Horde.DynamicSupervisor ignores the `id` of a child spec (as Elixir.DynamicSupervisor does), and therefore does not guarantee that each `id` will be unique in the cluster (as it did pre-0.6.0). If you want to uniquely name your processes in a cluster, use Horde.Registry for this purpose. Having both Horde.DynamicSupervisor and Horde.Registry checking for uniqueness was subject to a race condition where Horde.DynamicSupervisor would choose process A to survive and Horde.Registry would choose process B to survive, resulting in both processes being killed._

## Graceful shutdown

Using `Horde.DynamicSupervisor.stop/3` will cause the local supervisor to stop and any processes it was running will be shut down and redistributed to remaining supervisors in the horde. (This should happen automatically if `:init.stop()` is called).

## Installation

Horde is [available in Hex](https://hex.pm/packages/horde).

The package can be installed by adding `horde` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:horde, "~> 0.8.5"}
  ]
end
```

## Usage

Here is a small taste of Horde's usage. See the full docs at [https://hexdocs.pm/horde](https://hexdocs.pm/horde) for more information and examples. There is also an example application at `examples/hello_world` that you can refer to if you get stuck.

Starting `Horde.DynamicSupervisor`:

```elixir
defmodule MyApp.Application do
  use Application
  def start(_type, _args) do
    children = [
      {Horde.DynamicSupervisor, [name: MyApp.DistributedSupervisor, strategy: :one_for_one]}
    ]
    Supervisor.start_link(children, strategy: :one_for_one)
  end
end
```

Adding a child to the supervisor:

```elixir
# Add a Task
Horde.DynamicSupervisor.start_child(MyApp.DistributedSupervisor, %{id: :task, start: {Task, :start_link, [:infinity]}})

# Add an Agent
Horde.DynamicSupervisor.start_child(MyApp.DistributedSupervisor, %{id: :agent, start: {Agent, :start_link, [fn -> %{} end]}})

# Add a GenServer: You need a previously defined GenServer to call the one
# liner below.  We have a test ("graceful shutdown") in
# `test/supervisor_test.exs` that exercises and displays that behavior. After
# defined, it would be very similar to this:
Horde.DynamicSupervisor.start_child(MyApp.DistributedSupervisor, %{id: :gen_server, start: {GenServer, :start_link, [DefinedGenServer, {500, pid}]}})
```

And so on. The public API should be the same as `Elixir.DynamicSupervisor` (and please open an issue if you find a difference).

Joining supervisors into a single distributed supervisor can be done using `Horde.Cluster`:

```elixir
{:ok, supervisor_1} = Horde.DynamicSupervisor.start_link(name: :distributed_supervisor_1, strategy: :one_for_one)
{:ok, supervisor_2} = Horde.DynamicSupervisor.start_link(name: :distributed_supervisor_2, strategy: :one_for_one)
{:ok, supervisor_3} = Horde.DynamicSupervisor.start_link(name: :distributed_supervisor_3, strategy: :one_for_one)

Horde.Cluster.set_members(:distributed_supervisor_1, [:distributed_supervisor_1, :distributed_supervisor_2, :distributed_supervisor_3])
# supervisor_1, supervisor_2 and supervisor_3 will be joined in a single cluster.
```


# Other projects

Useful libraries that use or extend Horde functionalities.

## [Horde.Process](https://github.com/tyler-eon/horde-process)

An opinionated but configurable means of quickly creating GenServer modules that are intended to be managed and distributed via Horde.

# Contributing

Contributions are welcome! Feel free to open an issue if you'd like to discuss a problem or a possible solution. Pull requests are much appreciated.

## Running the tests

Run `mix deps.get` and `mix test`. The test tooling requires OTP 25 or later
and Elixir 1.15 or later; this does not change Horde's runtime requirements.
CI covers OTP 25–29 with compatible Elixir versions. OTP 24 is no longer in
the test matrix because it does not provide `:peer`.

Distributed tests use `:peer` with a standard-I/O control channel. The nodes
remain alive when Erlang distribution is disconnected, and observations and
cleanup do not reconnect them. The two-node and four-node recovery scenarios
keep the test manager outside the distribution network; tests that need a local
caller explicitly retain its connection through the helper's default option.
Partition tests give each component a different cookie to prevent automatic reconnection, then restore the cookies on healing.
They keep `prevent_overlapping_partitions` enabled and verify the actual topology
and process identities before checking recovery. Do not disable this protection
in `ERL_FLAGS` or `ELIXIR_ERL_OPTIONS`; the suite rejects that configuration.

Cuts are sequential, so OTP can remove working connections within the intended
components while the split forms. The helper reconnects those components and
verifies their final topology. This is fragmentation followed by recovery to a
2+2 split, not an atomic network cut. Each peer records node-up/down events with
reasons and timestamps, included in failure snapshots. Earlier failure runs
also had the manager as a fifth visible node; removing it improves the fault
model but does not itself demonstrate that the recovery defect is repaired.

The previously skipped two-node and four-node partition scenarios are covered by
enabled tests with bounded convergence checks. The four-node scenario uses 20
named workers to test availability and duplicate resolution, not the former
1,000-worker stress workload. Workers follow the documented transient-restart
and registry-conflict protocol. These tests distinguish a surviving partition
from node death; they do not promise single ownership during a partition or
exhaustively explore every network failure.

### Known recovery failure exposed by the enabled suite

The four-node test remains capable of failing during partition formation, with
some supervisor child records missing and registry entries pointing to the other
component after the topology has settled. CI run
[36640977093](https://github.com/evadne/horde/actions/runs/36640977093) captured
this on OTP 25, 26, 28 and 29. The owner-specific registry cleanup repair addresses
a separately reproduced deletion race; it does not establish that this broader
recovery failure is fixed. Failed tests print each peer's topology, membership,
children and CRDT registrations for diagnosis. The assertion stays enabled.

Local passing runs and a subsequent green matrix do not invalidate that evidence.
Partition formation can involve intermediate disconnections and concurrent child
replacement; the precise loss mechanism still needs a deterministic reproduction.
Do not interpret this integration branch as establishing complete partition
recovery or as production rollout approval.
