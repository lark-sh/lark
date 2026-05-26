import type { DatabaseEvent } from '../api/client';

interface EventsLogProps {
  events: DatabaseEvent[];
  loading?: boolean;
}

function formatRelativeTime(ts: string): string {
  const date = new Date(ts);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffSec = Math.floor(diffMs / 1000);
  const diffMin = Math.floor(diffSec / 60);
  const diffHour = Math.floor(diffMin / 60);
  const diffDay = Math.floor(diffHour / 24);

  if (diffSec < 60) return 'just now';
  if (diffMin < 60) return `${diffMin}m ago`;
  if (diffHour < 24) return `${diffHour}h ago`;
  if (diffDay < 7) return `${diffDay}d ago`;
  return date.toLocaleDateString();
}

function eventStyle(eventType: string): { icon: string; color: string } {
  switch (eventType) {
    case 'high_latency':
      return { icon: '🐢', color: 'text-yellow-600 bg-yellow-50' };
    case 'approaching_ccu_limit':
      return { icon: '⚠️', color: 'text-orange-600 bg-orange-50' };
    case 'connection_rejected':
      return { icon: '🚫', color: 'text-red-600 bg-red-50' };
    case 'storage_warning':
      return { icon: '💾', color: 'text-purple-600 bg-purple-50' };
    default:
      return { icon: 'ℹ️', color: 'text-blue-600 bg-blue-50' };
  }
}

export function EventsLog({ events, loading }: EventsLogProps) {
  return (
    <div className="bg-white rounded-lg border border-gray-200 p-4">
      <h3 className="text-sm font-medium text-gray-700 mb-4">Database events</h3>
      {loading ? (
        <div className="text-sm text-gray-500">Loading events…</div>
      ) : events.length === 0 ? (
        <div className="text-center py-4 text-sm text-gray-500">No events recently</div>
      ) : (
        <div className="space-y-2">
          {events.map((event) => {
            const { icon, color } = eventStyle(event.event_type);
            return (
              <div
                key={event.id}
                className="flex items-start gap-3 py-2 border-b border-gray-100 last:border-0"
              >
                <span
                  className={
                    'flex-shrink-0 w-8 h-8 flex items-center justify-center rounded-full ' +
                    color
                  }
                >
                  {icon}
                </span>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-gray-500">
                      {formatRelativeTime(event.ts)}
                    </span>
                    <span className="text-xs font-mono text-gray-600 truncate">
                      {event.database_id}
                    </span>
                  </div>
                  <p className="text-sm text-gray-900 mt-0.5">{event.message}</p>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
