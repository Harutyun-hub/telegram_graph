import { useEffect, useId, useState } from 'react';
import { Info } from 'lucide-react';
import type { AdminWidgetId } from '../../admin/catalog';
import { getWidgetExplanation, getWidgetLabel } from '../../dashboard/widgetExplanations';
import { useLanguage } from '../../contexts/LanguageContext';
import { cn } from './utils';
import { useIsMobile } from './use-mobile';
import { Tooltip, TooltipContent, TooltipTrigger } from './tooltip';
import { Popover, PopoverContent, PopoverTrigger } from './popover';

interface WidgetInfoTriggerProps {
  widgetId: AdminWidgetId;
  label?: string;
  className?: string;
}

export function WidgetInfoTrigger({ widgetId, label, className }: WidgetInfoTriggerProps) {
  const { lang } = useLanguage();
  const isMobile = useIsMobile();
  const [popoverOpen, setPopoverOpen] = useState(false);
  const [tooltipOpen, setTooltipOpen] = useState(false);
  const contentId = useId();
  const explanation = getWidgetExplanation(widgetId, lang);
  const title = label || getWidgetLabel(widgetId, lang);

  useEffect(() => {
    if (popoverOpen) {
      setTooltipOpen(false);
    }
  }, [popoverOpen]);

  const tooltipEnabled = !isMobile && !popoverOpen;
  const ariaLabel = lang === 'ru'
    ? `Что показывает виджет «${title}»`
    : `Explain what the ${title} widget measures`;

  return (
    <Popover open={popoverOpen} onOpenChange={setPopoverOpen}>
      <Tooltip
        open={tooltipEnabled ? tooltipOpen : false}
        onOpenChange={(nextOpen) => {
          setTooltipOpen(tooltipEnabled ? nextOpen : false);
        }}
      >
        <TooltipTrigger asChild>
          <PopoverTrigger asChild>
            <button
              type="button"
              aria-label={ariaLabel}
              aria-haspopup="dialog"
              aria-expanded={popoverOpen}
              aria-controls={popoverOpen ? contentId : undefined}
              className={cn(
                'inline-flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full border border-slate-200 bg-white text-slate-400 transition-colors hover:border-sky-200 hover:text-sky-600 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-sky-500/30 focus-visible:ring-offset-2',
                className,
              )}
            >
              <Info className="h-3.5 w-3.5" />
            </button>
          </PopoverTrigger>
        </TooltipTrigger>
        <TooltipContent side="top" sideOffset={8} className="max-w-[260px] rounded-lg bg-slate-900 px-3 py-2 text-[11px] leading-relaxed text-white shadow-lg">
          {explanation.short}
        </TooltipContent>
      </Tooltip>

      <PopoverContent
        id={contentId}
        align="start"
        sideOffset={10}
        className="w-[min(24rem,calc(100vw-2rem))] rounded-2xl border border-slate-200 bg-white p-0 shadow-[0_20px_55px_rgba(15,23,42,0.16)]"
      >
        <div className="rounded-2xl bg-gradient-to-br from-sky-50 via-white to-white p-4">
          <div className="flex items-start gap-3">
            <div className="mt-0.5 flex h-9 w-9 flex-shrink-0 items-center justify-center rounded-xl bg-sky-100 text-sky-700">
              <Info className="h-4 w-4" />
            </div>
            <div className="min-w-0 flex-1">
              <p className="text-[11px] uppercase tracking-[0.14em] text-slate-400">
                {lang === 'ru' ? 'О виджете' : 'Widget guide'}
              </p>
              <h4 className="mt-1 text-sm font-semibold text-slate-900">
                {title}
              </h4>
              <p className="mt-2 text-sm leading-6 text-slate-600">
                {explanation.overview}
              </p>
            </div>
          </div>

          <div className="mt-4 rounded-xl border border-slate-100 bg-white/90 p-3">
            <p className="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-400">
              {lang === 'ru' ? 'На чём основано' : 'What it uses'}
            </p>
            <ul className="mt-2 space-y-2">
              {explanation.metrics.map((metric) => (
                <li key={metric} className="flex gap-2 text-sm leading-5 text-slate-600">
                  <span className="mt-[7px] h-1.5 w-1.5 flex-shrink-0 rounded-full bg-sky-500" />
                  <span>{metric}</span>
                </li>
              ))}
            </ul>
          </div>

          {explanation.note && (
            <div className="mt-3 rounded-xl border border-sky-100 bg-sky-50/80 px-3 py-2.5">
              <p className="text-[11px] font-semibold uppercase tracking-[0.12em] text-sky-700/70">
                {lang === 'ru' ? 'Важно' : 'Helpful note'}
              </p>
              <p className="mt-1 text-sm leading-5 text-slate-700">
                {explanation.note}
              </p>
            </div>
          )}
        </div>
      </PopoverContent>
    </Popover>
  );
}
