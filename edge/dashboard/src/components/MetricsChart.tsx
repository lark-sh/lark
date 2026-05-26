import { useMemo } from 'react';
import {
  CategoryScale,
  Chart as ChartJS,
  Filler,
  Legend,
  LinearScale,
  LineElement,
  PointElement,
  Title,
  Tooltip,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import type { TimeseriesPoint } from '../api/client';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  Filler,
);

export type MetricType = 'ccu' | 'bandwidth' | 'operations' | 'latency';

interface MetricsChartProps {
  timeseries: TimeseriesPoint[];
  metric: MetricType;
}

function formatTime(ts: string): string {
  const d = new Date(ts);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toString();
}

export function MetricsChart({ timeseries, metric }: MetricsChartProps) {
  const data = useMemo(() => {
    const points = [...timeseries];
    const labels = points.map((p) => formatTime(p.ts));

    switch (metric) {
      case 'ccu':
        return {
          labels,
          datasets: [
            {
              label: 'Concurrent users',
              data: points.map((p) => p.ccu),
              borderColor: 'rgb(59, 130, 246)',
              backgroundColor: 'rgba(59, 130, 246, 0.1)',
              fill: true,
              tension: 0.3,
            },
          ],
        };
      case 'bandwidth':
        return {
          labels,
          datasets: [
            {
              label: 'Bytes in',
              data: points.map((p) => p.bytes_in),
              borderColor: 'rgb(34, 197, 94)',
              backgroundColor: 'rgba(34, 197, 94, 0.1)',
              fill: true,
              tension: 0.3,
            },
            {
              label: 'Bytes out',
              data: points.map((p) => p.bytes_out),
              borderColor: 'rgb(168, 85, 247)',
              backgroundColor: 'rgba(168, 85, 247, 0.1)',
              fill: true,
              tension: 0.3,
            },
          ],
        };
      case 'operations':
        return {
          labels,
          datasets: [
            {
              label: 'Writes',
              data: points.map((p) => p.writes),
              borderColor: 'rgb(249, 115, 22)',
              backgroundColor: 'rgba(249, 115, 22, 0.1)',
              fill: false,
              tension: 0.3,
            },
            {
              label: 'Reads',
              data: points.map((p) => p.reads),
              borderColor: 'rgb(14, 165, 233)',
              backgroundColor: 'rgba(14, 165, 233, 0.1)',
              fill: false,
              tension: 0.3,
            },
            {
              label: 'Events sent',
              data: points.map((p) => p.events_sent),
              borderColor: 'rgb(34, 197, 94)',
              backgroundColor: 'rgba(34, 197, 94, 0.1)',
              fill: false,
              tension: 0.3,
            },
          ],
        };
      case 'latency':
        return {
          labels,
          datasets: [
            {
              label: 'Avg processing time (ms)',
              data: points.map((p) => p.p50_latency_us / 1000),
              borderColor: 'rgb(139, 92, 246)',
              backgroundColor: 'rgba(139, 92, 246, 0.1)',
              fill: true,
              tension: 0.3,
            },
          ],
        };
      default:
        return { labels: [], datasets: [] };
    }
  }, [timeseries, metric]);

  const options = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: metric === 'bandwidth' || metric === 'operations',
          position: 'top' as const,
        },
        tooltip: {
          callbacks: {
            label: (context: { dataset: { label?: string }; raw: unknown }) => {
              const value = context.raw as number;
              if (metric === 'bandwidth') {
                return `${context.dataset.label}: ${formatBytes(value)}`;
              }
              if (metric === 'latency') {
                return `${context.dataset.label}: ${value.toFixed(1)}ms`;
              }
              return `${context.dataset.label}: ${formatNumber(value)}`;
            },
          },
        },
      },
      scales: {
        y: {
          beginAtZero: true,
          ticks: {
            callback: (value: number | string) => {
              const n = typeof value === 'string' ? parseFloat(value) : value;
              if (metric === 'bandwidth') return formatBytes(n);
              if (metric === 'latency') return `${n}ms`;
              return formatNumber(n);
            },
          },
        },
        x: {
          ticks: { maxTicksLimit: 12 },
        },
      },
      interaction: {
        intersect: false,
        mode: 'index' as const,
      },
    }),
    [metric],
  );

  if (timeseries.length === 0) {
    return (
      <div className="h-64 flex items-center justify-center text-sm text-gray-500">
        No metrics for the selected time range.
      </div>
    );
  }

  return (
    <div className="h-64">
      <Line data={data} options={options} />
    </div>
  );
}
