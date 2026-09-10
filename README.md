# Event Sourcing Stream

A TypeScript append-only event log with deterministic replay and projections. Build event-sourced applications with snapshotting, projection management, and full replay capabilities.

## Features

- **Append-Only Event Log**: Immutable event storage with automatic file rotation
- **Projections**: Register reducers to build materialized views from events
- **Snapshots**: Automatic snapshotting with SHA256 checksum verification
- **Deterministic Replay**: Rebuild projections from event history
- **Correlation Tracking**: Link related events via correlation and causation IDs
- **File-Based Storage**: JSONL format for human-readable, debuggable storage
- **Type-Safe**: Full TypeScript generics support

## Installation

```bash
npm install
```

## Usage

```typescript
import { EventSourcingStream, Projection } from './event_sourcing_stream';

const stream = new EventSourcingStream({
  storagePath: './event-store',
  snapshotInterval: 100,
});

// Define a projection
const counterProjection: Projection<number> = {
  name: 'counter',
  initialState: 0,
  reducer: (state, event) => {
    if (event.type === 'INCREMENT') return state + event.payload.amount;
    return state;
  },
};

stream.registerProjection(counterProjection);
stream.append('INCREMENT', { amount: 5 });

console.log(stream.getProjectionState('counter')); // 5
stream.close();
```

## Testing

```bash
npx tsc --noEmit
```

## License

MIT


## Prerequisites
- Required environment and dependencies

