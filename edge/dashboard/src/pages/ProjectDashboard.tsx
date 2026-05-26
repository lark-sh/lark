import { useEffect, useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { api, ApiError, type DashboardData } from '../api/client';
import { StatsCard } from '../components/StatsCard';
import { MetricsChart, type MetricType } from '../components/MetricsChart';
import { EventsLog } from '../components/EventsLog';
import { DatabaseList } from '../components/DatabaseList';

type TimeRange = '1h' | '24h' | '7d';

const RANGE_MS: Record<TimeRange, number> = {
  '1h': 60 * 60 * 1000,
  '24h': 24 * 60 * 60 * 1000,
  '7d': 7 * 24 * 60 * 60 * 1000,
};

const RANGE_LABEL: Record<TimeRange, string> = {
  '1h': 'last hour',
  '24h': 'last 24 hours',
  '7d': 'last 7 days',
};

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toLocaleString();
}

export function ProjectDashboard() {
  const { projectId } = useParams<{ projectId: string }>();

  const [dashboard, setDashboard] = useState<DashboardData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [metric, setMetric] = useState<MetricType>('ccu');
  const [range, setRange] = useState<TimeRange>('24h');

  useEffect(() => {
    if (!projectId) return;
    let cancelled = false;
    (async () => {
      setLoading(true);
      setError(null);
      try {
        const now = new Date();
        const start = new Date(now.getTime() - RANGE_MS[range]);
        const data = await api.getDashboard(projectId, {
          start: start.toISOString(),
          end: now.toISOString(),
        });
        if (!cancelled) setDashboard(data);
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof ApiError ? err.message : 'Failed to load dashboard');
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [projectId, range]);

  if (loading && !dashboard) {
    return <div className="text-sm text-gray-500">Loading dashboard…</div>;
  }
  if (error && !dashboard) {
    return (
      <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-md text-sm">
        {error}
      </div>
    );
  }
  if (!dashboard || !projectId) {
    return <div className="text-sm text-gray-500">Project not found.</div>;
  }

  const { summary, timeseries, recent_events } = dashboard;
  const totalBandwidth =
    (summary?.total_bytes_in ?? 0) + (summary?.total_bytes_out ?? 0);
  const totalOps = (summary?.total_writes ?? 0) + (summary?.total_reads ?? 0);
  const avgLatencyMs = summary?.avg_latency_us ? summary.avg_latency_us / 1000 : 0;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <Link to="/projects" className="text-sm text-gray-500 hover:text-gray-700">
            ← Projects
          </Link>
          <h1 className="text-2xl font-semibold text-gray-900 mt-1">
            {dashboard.project.name}
          </h1>
        </div>
        <Link
          to={`/projects/${projectId}/settings`}
          className="px-4 py-2 text-sm text-gray-700 border border-gray-300 rounded-md hover:bg-gray-50"
        >
          Settings
        </Link>
      </div>

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-md text-sm">
          {error}
        </div>
      )}

      {/* Stats cards (label + grid in one wrapper so space-y-6 on the
          parent treats them as a single unit, not two siblings with 24px
          between them). */}
      <div>
        <div className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">
          {RANGE_LABEL[range]}
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <StatsCard
            title="Peak CCU"
            value={formatNumber(summary?.peak_ccu ?? 0)}
            active={metric === 'ccu'}
            onClick={() => setMetric('ccu')}
          />
          <StatsCard
            title="Bandwidth"
            value={formatBytes(totalBandwidth)}
            subtitle={`${formatBytes(summary?.total_bytes_in ?? 0)} in / ${formatBytes(summary?.total_bytes_out ?? 0)} out`}
            active={metric === 'bandwidth'}
            onClick={() => setMetric('bandwidth')}
          />
          <StatsCard
            title="Operations"
            value={formatNumber(totalOps)}
            subtitle={`${formatNumber(summary?.total_writes ?? 0)} writes / ${formatNumber(summary?.total_reads ?? 0)} reads / ${formatNumber(summary?.total_events ?? 0)} events`}
            active={metric === 'operations'}
            onClick={() => setMetric('operations')}
          />
          <StatsCard
            title="Processing"
            value={`${avgLatencyMs.toFixed(1)}ms`}
            subtitle="avg per operation"
            active={metric === 'latency'}
            onClick={() => setMetric('latency')}
          />
        </div>
      </div>

      {/* Chart */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <div className="flex items-center justify-between mb-4">
          <div className="flex gap-1">
            {(['ccu', 'bandwidth', 'operations', 'latency'] as MetricType[]).map((m) => {
              const labels: Record<MetricType, string> = {
                ccu: 'CCU',
                bandwidth: 'Bandwidth',
                operations: 'Operations',
                latency: 'Processing',
              };
              return (
                <button
                  key={m}
                  type="button"
                  onClick={() => setMetric(m)}
                  className={
                    'px-3 py-1.5 text-sm rounded-md transition-colors ' +
                    (metric === m
                      ? 'bg-blue-100 text-blue-700 font-medium'
                      : 'text-gray-600 hover:bg-gray-100')
                  }
                >
                  {labels[m]}
                </button>
              );
            })}
          </div>
          <div className="flex gap-1">
            {(['1h', '24h', '7d'] as TimeRange[]).map((r) => (
              <button
                key={r}
                type="button"
                onClick={() => setRange(r)}
                className={
                  'px-3 py-1.5 text-sm rounded-md transition-colors ' +
                  (range === r
                    ? 'bg-gray-200 text-gray-900 font-medium'
                    : 'text-gray-600 hover:bg-gray-100')
                }
              >
                {r}
              </button>
            ))}
          </div>
        </div>

        <MetricsChart timeseries={timeseries} metric={metric} />
      </div>

      <EventsLog events={recent_events ?? []} />

      <DatabaseList projectId={projectId} />
    </div>
  );
}
