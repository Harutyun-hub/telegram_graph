import type { ReactNode } from 'react';
import type { AdminWidgetId } from '../../admin/catalog';
import { cn } from './utils';
import { WidgetInfoTrigger } from './WidgetInfoTrigger';

interface WidgetTitleProps {
  widgetId: AdminWidgetId;
  children: ReactNode;
  className?: string;
  headingClassName?: string;
}

export function WidgetTitle({ widgetId, children, className, headingClassName }: WidgetTitleProps) {
  const label = typeof children === 'string' ? children : undefined;

  return (
    <div className={cn('flex min-w-0 flex-wrap items-start gap-2', className)}>
      <h3 className={cn('min-w-0 flex-1 leading-tight text-gray-900', headingClassName)} style={{ fontSize: '1.05rem' }}>
        {children}
      </h3>
      <WidgetInfoTrigger widgetId={widgetId} label={label} className="mt-0.5 self-start" />
    </div>
  );
}
