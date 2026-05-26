interface StatsCardProps {
  title: string;
  value: string;
  subtitle?: string;
  active?: boolean;
  onClick?: () => void;
}

export function StatsCard({ title, value, subtitle, active, onClick }: StatsCardProps) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={
        'flex-1 p-4 rounded-lg border text-left transition-colors ' +
        (active
          ? 'bg-blue-50 border-blue-300'
          : 'bg-white border-gray-200 hover:border-gray-300') +
        (onClick ? ' cursor-pointer' : ' cursor-default')
      }
    >
      <div className="text-sm font-medium text-gray-500">{title}</div>
      <div className="mt-1 text-2xl font-semibold text-gray-900">{value}</div>
      {subtitle && <div className="mt-1 text-sm text-gray-500">{subtitle}</div>}
    </button>
  );
}
