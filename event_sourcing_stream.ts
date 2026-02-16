/**
 * Local Event Sourcing Stream
 * Append-only event log with deterministic replay and projections
 */

import * as crypto from 'crypto';
import * as fs from 'fs';
import * as path from 'path';

// ============================================================================
// Core Types
// ============================================================================

export interface Event<TPayload = any> {
  id: string;
  type: string;
  payload: TPayload;
  metadata: EventMetadata;
  timestamp: number;
  sequence: number;
}

export interface EventMetadata {
  correlationId?: string;
  causationId?: string;
  userId?: string;
  [key: string]: any;
}

export interface Snapshot<TState = any> {
  sequence: number;
  timestamp: number;
  state: TState;
  checksum: string;
}

export type Reducer<TState = any, TPayload = any> = (
  state: TState,
  event: Event<TPayload>
) => TState;

export interface Projection<TState = any> {
  name: string;
  initialState: TState;
  reducer: Reducer<TState>;
  snapshotInterval?: number;
}

export interface StreamConfig {
  storagePath: string;
  snapshotInterval?: number;
  compressionEnabled?: boolean;
}

// ============================================================================
// Event Store
// ============================================================================

export class EventStore {
  private storagePath: string;
  private eventsPath: string;
  private snapshotsPath: string;
  private sequence: number = 0;
  private events: Event[] = [];
  private writeStream: fs.WriteStream | null = null;

  constructor(config: StreamConfig) {
    this.storagePath = config.storagePath;
    this.eventsPath = path.join(this.storagePath, 'events');
    this.snapshotsPath = path.join(this.storagePath, 'snapshots');

    this.ensureDirectories();
    this.loadEvents();
  }

  private ensureDirectories(): void {
    if (!fs.existsSync(this.storagePath)) {
      fs.mkdirSync(this.storagePath, { recursive: true });
    }
    if (!fs.existsSync(this.eventsPath)) {
      fs.mkdirSync(this.eventsPath, { recursive: true });
    }
    if (!fs.existsSync(this.snapshotsPath)) {
      fs.mkdirSync(this.snapshotsPath, { recursive: true });
    }
  }

  private loadEvents(): void {
    const eventFiles = fs.readdirSync(this.eventsPath)
      .filter(f => f.endsWith('.jsonl'))
      .sort();

    for (const file of eventFiles) {
      const filePath = path.join(this.eventsPath, file);
      const content = fs.readFileSync(filePath, 'utf-8');
      const lines = content.trim().split('\n').filter(l => l.length > 0);

      for (const line of lines) {
        try {
          const event = JSON.parse(line) as Event;
          this.events.push(event);
          if (event.sequence >= this.sequence) {
            this.sequence = event.sequence + 1;
          }
        } catch (error) {
          console.error('Failed to parse event:', error);
        }
      }
    }
  }

  private getCurrentEventFile(): string {
    const date = new Date().toISOString().split('T')[0];
    return path.join(this.eventsPath, `events-${date}.jsonl`);
  }

  private getWriteStream(): fs.WriteStream {
    const currentFile = this.getCurrentEventFile();

    if (this.writeStream && this.writeStream.path === currentFile) {
      return this.writeStream;
    }

    if (this.writeStream) {
      this.writeStream.end();
    }

    this.writeStream = fs.createWriteStream(currentFile, { flags: 'a' });
    return this.writeStream;
  }

  public append<TPayload = any>(
    type: string,
    payload: TPayload,
    metadata: EventMetadata = {}
  ): Event<TPayload> {
    const event: Event<TPayload> = {
      id: this.generateEventId(),
      type,
      payload,
      metadata,
      timestamp: Date.now(),
      sequence: this.sequence++,
    };

    // Write to disk
    const stream = this.getWriteStream();
    stream.write(JSON.stringify(event) + '\n');

    // Store in memory
    this.events.push(event as Event);

    return event;
  }

  public getEvents(fromSequence: number = 0, toSequence?: number): Event[] {
    let filtered = this.events.filter(e => e.sequence >= fromSequence);

    if (toSequence !== undefined) {
      filtered = filtered.filter(e => e.sequence <= toSequence);
    }

    return filtered;
  }

  public getEventsByType(type: string): Event[] {
    return this.events.filter(e => e.type === type);
  }

  public getEventsByCorrelation(correlationId: string): Event[] {
    return this.events.filter(e => e.metadata.correlationId === correlationId);
  }

  public getCurrentSequence(): number {
    return this.sequence;
  }

  public getTotalEvents(): number {
    return this.events.length;
  }

  private generateEventId(): string {
    return crypto.randomBytes(16).toString('hex');
  }

  public close(): void {
    if (this.writeStream) {
      this.writeStream.end();
      this.writeStream = null;
    }
  }
}

// ============================================================================
// Projection Manager
// ============================================================================

export class ProjectionManager<TState = any> {
  private eventStore: EventStore;
  private projections: Map<string, Projection<any>> = new Map();
  private states: Map<string, any> = new Map();
  private lastProcessedSequence: Map<string, number> = new Map();

  constructor(eventStore: EventStore) {
    this.eventStore = eventStore;
  }

  public registerProjection<TState>(projection: Projection<TState>): void {
    this.projections.set(projection.name, projection);
    this.states.set(projection.name, projection.initialState);
    this.lastProcessedSequence.set(projection.name, -1);

    // Try to load snapshot
    this.loadSnapshot(projection.name);
  }

  public getState<TState>(projectionName: string): TState | undefined {
    return this.states.get(projectionName);
  }

  public rebuild(projectionName: string): void {
    const projection = this.projections.get(projectionName);
    if (!projection) {
      throw new Error(`Projection ${projectionName} not found`);
    }

    this.states.set(projectionName, projection.initialState);
    this.lastProcessedSequence.set(projectionName, -1);
    this.processEvents(projectionName);
  }

  public rebuildAll(): void {
    for (const projectionName of this.projections.keys()) {
      this.rebuild(projectionName);
    }
  }

  public processEvents(projectionName?: string): void {
    const projectionsToProcess = projectionName
      ? [projectionName]
      : Array.from(this.projections.keys());

    for (const name of projectionsToProcess) {
      this.processProjection(name);
    }
  }

  private processProjection(projectionName: string): void {
    const projection = this.projections.get(projectionName);
    if (!projection) {
      return;
    }

    const lastSequence = this.lastProcessedSequence.get(projectionName) ?? -1;
    const events = this.eventStore.getEvents(lastSequence + 1);

    let state = this.states.get(projectionName);

    for (const event of events) {
      state = projection.reducer(state, event);
      this.lastProcessedSequence.set(projectionName, event.sequence);
    }

    this.states.set(projectionName, state);

    // Create snapshot if needed
    if (projection.snapshotInterval) {
      const eventsSinceSnapshot = events.length;
      if (eventsSinceSnapshot >= projection.snapshotInterval) {
        this.createSnapshot(projectionName);
      }
    }
  }

  private createSnapshot(projectionName: string): void {
    const state = this.states.get(projectionName);
    const sequence = this.lastProcessedSequence.get(projectionName) ?? -1;

    if (state === undefined || sequence < 0) {
      return;
    }

    const snapshot: Snapshot = {
      sequence,
      timestamp: Date.now(),
      state,
      checksum: this.calculateChecksum(state),
    };

    const snapshotPath = path.join(
      this.eventStore['snapshotsPath'],
      `${projectionName}-${sequence}.json`
    );

    fs.writeFileSync(snapshotPath, JSON.stringify(snapshot, null, 2));
  }

  private loadSnapshot(projectionName: string): boolean {
    const snapshotDir = this.eventStore['snapshotsPath'];
    const snapshotFiles = fs.readdirSync(snapshotDir)
      .filter(f => f.startsWith(`${projectionName}-`) && f.endsWith('.json'))
      .sort()
      .reverse();

    if (snapshotFiles.length === 0) {
      return false;
    }

    const latestSnapshot = snapshotFiles[0];
    const snapshotPath = path.join(snapshotDir, latestSnapshot);

    try {
      const snapshot: Snapshot = JSON.parse(fs.readFileSync(snapshotPath, 'utf-8'));

      // Verify checksum
      const calculatedChecksum = this.calculateChecksum(snapshot.state);
      if (calculatedChecksum !== snapshot.checksum) {
        console.error('Snapshot checksum mismatch, rebuilding from events');
        return false;
      }

      this.states.set(projectionName, snapshot.state);
      this.lastProcessedSequence.set(projectionName, snapshot.sequence);

      return true;
    } catch (error) {
      console.error('Failed to load snapshot:', error);
      return false;
    }
  }

  private calculateChecksum(data: any): string {
    const json = JSON.stringify(data);
    return crypto.createHash('sha256').update(json).digest('hex');
  }

  public getProjectionInfo(projectionName: string) {
    return {
      name: projectionName,
      lastProcessedSequence: this.lastProcessedSequence.get(projectionName),
      state: this.states.get(projectionName),
    };
  }

  public getAllProjectionsInfo() {
    return Array.from(this.projections.keys()).map(name =>
      this.getProjectionInfo(name)
    );
  }
}

// ============================================================================
// Event Sourcing Stream (Main Interface)
// ============================================================================

export class EventSourcingStream {
  private eventStore: EventStore;
  private projectionManager: ProjectionManager;

  constructor(config: StreamConfig) {
    this.eventStore = new EventStore(config);
    this.projectionManager = new ProjectionManager(this.eventStore);
  }

  public append<TPayload = any>(
    type: string,
    payload: TPayload,
    metadata: EventMetadata = {}
  ): Event<TPayload> {
    const event = this.eventStore.append(type, payload, metadata);
    this.projectionManager.processEvents();
    return event;
  }

  public registerProjection<TState>(projection: Projection<TState>): void {
    this.projectionManager.registerProjection(projection);
    this.projectionManager.processEvents(projection.name);
  }

  public getProjectionState<TState>(projectionName: string): TState | undefined {
    return this.projectionManager.getState<TState>(projectionName);
  }

  public getEvents(fromSequence: number = 0, toSequence?: number): Event[] {
    return this.eventStore.getEvents(fromSequence, toSequence);
  }

  public getEventsByType(type: string): Event[] {
    return this.eventStore.getEventsByType(type);
  }

  public replay(): void {
    this.projectionManager.rebuildAll();
  }

  public getStats() {
    return {
      totalEvents: this.eventStore.getTotalEvents(),
      currentSequence: this.eventStore.getCurrentSequence(),
      projections: this.projectionManager.getAllProjectionsInfo(),
    };
  }

  public close(): void {
    this.eventStore.close();
  }
}

// ============================================================================
// Example Usage
// ============================================================================

// Example: Shopping Cart Event Sourcing

interface CartState {
  items: Map<string, { name: string; price: number; quantity: number }>;
  total: number;
}

interface UserState {
  id: string;
  name: string;
  email: string;
  createdAt: number;
  updatedAt: number;
}

function main() {
  // Initialize stream
  const stream = new EventSourcingStream({
    storagePath: './event-store',
    snapshotInterval: 100,
  });

  // Define projection
  const cartProjection: Projection<CartState> = {
    name: 'shopping-cart',
    initialState: {
      items: new Map(),
      total: 0,
    },
    reducer: (state, event) => {
      switch (event.type) {
        case 'ITEM_ADDED': {
          const { itemId, name, price, quantity } = event.payload;
          const newItems = new Map(state.items);

          const existing = newItems.get(itemId);
          if (existing) {
            existing.quantity += quantity;
          } else {
            newItems.set(itemId, { name, price, quantity });
          }

          const total = Array.from(newItems.values()).reduce(
            (sum, item) => sum + item.price * item.quantity,
            0
          );

          return { items: newItems, total };
        }

        case 'ITEM_REMOVED': {
          const { itemId } = event.payload;
          const newItems = new Map(state.items);
          newItems.delete(itemId);

          const total = Array.from(newItems.values()).reduce(
            (sum, item) => sum + item.price * item.quantity,
            0
          );

          return { items: newItems, total };
        }

        case 'CART_CLEARED': {
          return {
            items: new Map(),
            total: 0,
          };
        }

        default:
          return state;
      }
    },
    snapshotInterval: 50,
  };

  // Register projection
  stream.registerProjection(cartProjection);

  // Append events
  stream.append('ITEM_ADDED', {
    itemId: '1',
    name: 'Widget',
    price: 29.99,
    quantity: 2,
  });

  stream.append('ITEM_ADDED', {
    itemId: '2',
    name: 'Gadget',
    price: 49.99,
    quantity: 1,
  });

  stream.append('ITEM_REMOVED', {
    itemId: '1',
  });

  // Get current state
  const cartState = stream.getProjectionState<CartState>('shopping-cart');
  console.log('Cart State:', {
    items: Array.from(cartState?.items.entries() || []),
    total: cartState?.total,
  });

  // Get stats
  console.log('Stream Stats:', stream.getStats());

  // User aggregate projection
  const userProjection: Projection<Map<string, UserState>> = {
    name: 'users',
    initialState: new Map(),
    reducer: (state, event) => {
      const newState = new Map(state);

      switch (event.type) {
        case 'USER_CREATED': {
          const { userId, name, email } = event.payload;
          newState.set(userId, {
            id: userId,
            name,
            email,
            createdAt: event.timestamp,
            updatedAt: event.timestamp,
          });
          break;
        }

        case 'USER_UPDATED': {
          const { userId, name, email } = event.payload;
          const user = newState.get(userId);
          if (user) {
            newState.set(userId, {
              ...user,
              name: name ?? user.name,
              email: email ?? user.email,
              updatedAt: event.timestamp,
            });
          }
          break;
        }

        case 'USER_DELETED': {
          const { userId } = event.payload;
          newState.delete(userId);
          break;
        }
      }

      return newState;
    },
    snapshotInterval: 100,
  };

  stream.registerProjection(userProjection);

  // Test user events
  stream.append('USER_CREATED', {
    userId: 'user-1',
    name: 'John Doe',
    email: 'john@example.com',
  });

  stream.append('USER_UPDATED', {
    userId: 'user-1',
    name: 'John Smith',
  });

  const usersState = stream.getProjectionState<Map<string, UserState>>('users');
  console.log('Users:', Array.from(usersState?.entries() || []));

  // Close stream
  stream.close();
}

// Only run example when executed directly
if (require.main === module) {
  main();
}

