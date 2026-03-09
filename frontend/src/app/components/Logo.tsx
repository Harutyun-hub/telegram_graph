interface LogoProps {
  size?: number;
  className?: string;
  variant?: 'default' | 'icon-only' | 'horizontal';
}

// Inline SVG logo — no external asset dependency, works in any build env
function RadarSVG({ size }: { size: number }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 40 40"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      style={{ display: 'block' }}
    >
      <defs>
        <linearGradient id="logoGrad" x1="0" y1="0" x2="40" y2="40" gradientUnits="userSpaceOnUse">
          <stop offset="0%" stopColor="#1a56db" />
          <stop offset="100%" stopColor="#1e3a8a" />
        </linearGradient>
      </defs>
      {/* Background circle */}
      <circle cx="20" cy="20" r="20" fill="url(#logoGrad)" />

      {/* Radar arcs */}
      <path
        d="M20 20 m-12 0 a12 12 0 0 1 24 0"
        stroke="white"
        strokeWidth="2"
        strokeLinecap="round"
        fill="none"
        opacity="0.4"
      />
      <path
        d="M20 20 m-8 0 a8 8 0 0 1 16 0"
        stroke="white"
        strokeWidth="2"
        strokeLinecap="round"
        fill="none"
        opacity="0.65"
      />
      <path
        d="M20 20 m-4 0 a4 4 0 0 1 8 0"
        stroke="white"
        strokeWidth="2"
        strokeLinecap="round"
        fill="none"
        opacity="0.9"
      />

      {/* Radar sweep line */}
      <line x1="20" y1="20" x2="28" y2="12" stroke="white" strokeWidth="1.5" strokeLinecap="round" opacity="0.85" />

      {/* Center dot */}
      <circle cx="20" cy="20" r="2.5" fill="white" />

      {/* Ping dot */}
      <circle cx="28" cy="12" r="2" fill="white" opacity="0.9" />
    </svg>
  );
}

export function Logo({ size = 40, className = '' }: LogoProps) {
  return (
    <div
      className={`rounded-full overflow-hidden flex-shrink-0 ${className}`}
      style={{ width: size, height: size }}
    >
      <RadarSVG size={size} />
    </div>
  );
}

// Icon-only variant for compact spaces — always circular
export function LogoIcon({ size = 32, className = '' }: { size?: number; className?: string }) {
  return (
    <div
      className={`rounded-full overflow-hidden flex-shrink-0 ${className}`}
      style={{ width: size, height: size }}
    >
      <RadarSVG size={size} />
    </div>
  );
}
