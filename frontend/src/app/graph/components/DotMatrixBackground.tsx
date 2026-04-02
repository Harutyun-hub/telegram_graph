export function DotMatrixBackground() {
  return (
    <div className="absolute inset-0 overflow-hidden pointer-events-none">
      <div
        className="absolute inset-0"
        style={{
          backgroundImage: 'radial-gradient(circle, rgba(163, 172, 184, 0.14) 0.9px, transparent 0.9px)',
          backgroundSize: '26px 26px',
          opacity: 0.28,
        }}
      />
      <div
        className="absolute inset-0"
        style={{
          backgroundImage: 'radial-gradient(circle, rgba(116, 124, 138, 0.18) 0.7px, transparent 0.7px)',
          backgroundSize: '52px 52px',
          backgroundPosition: '13px 15px',
          opacity: 0.12,
        }}
      />
      <div
        className="absolute inset-0"
        style={{
          background: 'radial-gradient(ellipse at center, transparent 0%, transparent 40%, rgba(11, 14, 20, 0.5) 70%, rgba(11, 14, 20, 0.9) 100%)',
        }}
      />
    </div>
  );
}
