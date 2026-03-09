Frontend Migration Guide: Existing Dashboard Update
Overview
Goal: Update your existing Brand-Topic dashboard to support the new multi-dimensional graph (15 node types) WITHOUT rebuilding from scratch.

Strategy: Backward-compatible changes with progressive enhancement.

Time Estimate: 2-4 hours for core migration, 1-2 days for enhancements.

Current State vs Target State
What You Have (Old System)
javascript
// API Response
{
  nodes: [
    {id: "brand_nike", name: "Nike", type: "brand"},
    {id: "topic_cashback", name: "Cashback", type: "topic"}
  ],
  links: [
    {source: "brand_nike", target: "topic_cashback", weight: 45}
  ]
}
What You're Getting (New System)
javascript
// API Response (SAME FORMAT, more types)
{
  nodes: [
    {id: "brand_armeconom", name: "Armeconom Bank", type: "brand"},
    {id: "product_mastercard", name: "Mastercard Cashback", type: "product", category: "Credit_Card"},
    {id: "audience_cashback_seekers", name: "Cashback Seekers", type: "audience"}
  ],
  links: [
    {source: "brand_armeconom", target: "product_mastercard", type: "OFFERS", weight: 100},
    {source: "product_mastercard", target: "audience_cashback_seekers", type: "POSITIONED_FOR", weight: 85}
  ]
}
Key Change: More type values (15 instead of 2), typed links, richer metadata.

Migration Steps
Step 1: Update Color Mapping (5 minutes)
Find your color mapping function (probably looks like this):

javascript
// OLD CODE
const getNodeColor = (node) => {
  if (node.type === 'brand') return '#00bcd4';
  if (node.type === 'topic') return '#ff9800';
  return '#999999';
};
NEW CODE (Add new types):

javascript
const getNodeColor = (node) => {
  const colors = {
    brand: '#00bcd4',      // Cyan (unchanged)
    topic: '#ff9800',      // Orange (unchanged)
    
    // NEW TYPES
    product: '#4caf50',    // Green
    audience: '#9c27b0',   // Purple
    painpoint: '#f44336',  // Red
    valueprop: '#ffeb3b',  // Yellow
    intent: '#3f51b5',     // Indigo
    competitor: '#e91e63', // Pink
    cta: '#795548',        // Brown
    platform: '#607d8b',   // Blue-grey
    format: '#009688',     // Teal
    engagement: '#8bc34a', // Light green
    sentiment: '#ffc107',  // Amber
    timeperiod: '#9e9e9e'  // Grey
  };
  return colors[node.type] || '#999999';
};
Test: Graph should now show 15 colors instead of 2 ✅

Step 2: Add Curated Default View (15 minutes)
Current problem: Your API returns ALL 2,697 nodes → Hairball

Solution: Filter to top 60 nodes on frontend

Find your data fetching code:

javascript
// OLD CODE
const fetchGraphData = async () => {
  const response = await fetch('/api/graph-data');
  const data = await response.json();
  setGraphData(data); // Shows ALL nodes
};
NEW CODE (Add filtering):

javascript
const fetchGraphData = async () => {
  const response = await fetch('/api/graph-data');
  const fullData = await response.json();
  
  // CURATED DEFAULT VIEW
  const curatedData = filterToCuratedView(fullData);
  
  setGraphData(curatedData);
  setFullData(fullData); // Save for "Show More"
};
const filterToCuratedView = (data) => {
  // Get top 3 brands by product count
  const brandProductCounts = {};
  data.links
    .filter(l => l.type === 'OFFERS')
    .forEach(l => {
      brandProductCounts[l.source] = (brandProductCounts[l.source] || 0) + 1;
    });
  
  const topBrands = Object.entries(brandProductCounts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 3)
    .map(([id]) => id);
  
  // Get their products
  const brandProducts = data.links
    .filter(l => l.type === 'OFFERS' && topBrands.includes(l.source))
    .slice(0, 15) // Top 15 products
    .map(l => l.target);
  
  // Get product audiences (high confidence)
  const productAudiences = data.links
    .filter(l => l.type === 'POSITIONED_FOR' && brandProducts.includes(l.source))
    .filter(l => (l.weight || 100) >= 70) // High confidence only
    .slice(0, 30)
    .map(l => l.target);
  
  // Get top pain points
  const topPainPoints = data.nodes
    .filter(n => n.type === 'painpoint')
    .sort((a, b) => (b.severity === 'high' ? 1 : 0) - (a.severity === 'high' ? 1 : 0))
    .slice(0, 10)
    .map(n => n.id);
  
  // Collect relevant node IDs
  const relevantNodeIds = new Set([
    ...topBrands,
    ...brandProducts,
    ...productAudiences,
    ...topPainPoints
  ]);
  
  // Filter nodes and links
  const filteredNodes = data.nodes.filter(n => relevantNodeIds.has(n.id));
  const filteredLinks = data.links.filter(l => 
    relevantNodeIds.has(l.source) && relevantNodeIds.has(l.target)
  );
  
  return {
    nodes: filteredNodes,
    links: filteredLinks
  };
};
Add "Show More" button:

javascript
<button onClick={() => setGraphData(fullData)}>
  Show All {fullData.nodes.length} Nodes
</button>
Test: Initial load shows ~60 nodes, "Show More" reveals all ✅

Step 3: Add Node Type Legend (10 minutes)
Add visual legend so users understand colors:

javascript
const NodeTypeLegend = () => {
  const types = [
    {type: 'brand', label: 'Banks', color: '#00bcd4'},
    {type: 'product', label: 'Products', color: '#4caf50'},
    {type: 'audience', label: 'Audiences', color: '#9c27b0'},
    {type: 'painpoint', label: 'Pain Points', color: '#f44336'},
    {type: 'topic', label: 'Topics', color: '#ff9800'}
  ];
  
  return (
    <div className="legend">
      {types.map(t => (
        <div key={t.type} className="legend-item">
          <span className="legend-dot" style={{backgroundColor: t.color}} />
          <span>{t.label}</span>
        </div>
      ))}
    </div>
  );
};
Test: Legend appears, matches node colors ✅

Step 4: Update Hover Tooltips (10 minutes)
Show richer metadata on hover:

javascript
// OLD CODE
const getNodeTooltip = (node) => {
  return `${node.name} (${node.type})`;
};
NEW CODE:

javascript
const getNodeTooltip = (node) => {
  switch(node.type) {
    case 'product':
      return `${node.name}\nCategory: ${node.category || 'N/A'}\n${node.details || ''}`;
    case 'audience':
      return `${node.name}\nType: ${node.segmentType || 'N/A'}`;
    case 'painpoint':
      return `${node.name}\nSeverity: ${node.severity || 'medium'}`;
    default:
      return `${node.name} (${node.type})`;
  }
};
Test: Hovering shows specific details per node type ✅

Step 5: Add Node Type Filters (20 minutes)
Let users toggle node types on/off:

javascript
const [visibleTypes, setVisibleTypes] = useState({
  brand: true,
  product: true,
  audience: true,
  painpoint: true,
  topic: false, // Hidden by default (noise)
  valueprop: false,
  intent: false
});
const toggleNodeType = (type) => {
  setVisibleTypes(prev => ({...prev, [type]: !prev[type]}));
};
// Filter graph data
const visibleGraphData = {
  nodes: graphData.nodes.filter(n => visibleTypes[n.type]),
  links: graphData.links.filter(l => {
    const sourceNode = graphData.nodes.find(n => n.id === l.source);
    const targetNode = graphData.nodes.find(n => n.id === l.target);
    return visibleTypes[sourceNode?.type] && visibleTypes[targetNode?.type];
  })
};
UI Component:

javascript
<div className="node-type-filters">
  <label>
    <input type="checkbox" checked={visibleTypes.product} onChange={() => toggleNodeType('product')} />
    Products
  </label>
  <label>
    <input type="checkbox" checked={visibleTypes.audience} onChange={() => toggleNodeType('audience')} />
    Audiences
  </label>
  <label>
    <input type="checkbox" checked={visibleTypes.painpoint} onChange={() => toggleNodeType('painpoint')} />
    Pain Points
  </label>
</div>
Test: Toggling checkboxes shows/hides node types ✅

Step 6: Update Inspector Panel (30 minutes)
When user clicks a node, show type-specific details:

javascript
const NodeInspector = ({node, connections}) => {
  if (!node) return null;
  
  return (
    <div className="inspector">
      <h2>{node.name}</h2>
      <span className="type-badge">{node.type}</span>
      
      {/* Product-specific */}
      {node.type === 'product' && (
        <>
          <div className="detail">
            <strong>Category:</strong> {node.category}
          </div>
          <div className="detail">
            <strong>Details:</strong> {node.details}
          </div>
          <div className="connections">
            <h3>Targets</h3>
            <ul>
              {connections.filter(c => c.type === 'audience').map(c => (
                <li key={c.id}>{c.name}</li>
              ))}
            </ul>
          </div>
        </>
      )}
      
      {/* Audience-specific */}
      {node.type === 'audience' && (
        <>
          <div className="detail">
            <strong>Segment Type:</strong> {node.segmentType}
          </div>
          <div className="connections">
            <h3>Targeted By</h3>
            <ul>
              {connections.filter(c => c.type === 'product').map(c => (
                <li key={c.id}>{c.name}</li>
              ))}
            </ul>
          </div>
        </>
      )}
      
      {/* Generic fallback */}
      <div className="connections">
        <h3>Connected To ({connections.length})</h3>
        <ul>
          {connections.slice(0, 10).map(c => (
            <li key={c.id}>{c.name} ({c.type})</li>
          ))}
        </ul>
      </div>
    </div>
  );
};
Test: Clicking nodes shows type-specific information ✅

Step 7: Add Insight Annotation (30 minutes)
Auto-generate competitive insight on load:

javascript
const generateInsight = (data) => {
  // Find competitive overlap
  const audienceConnections = {};
  data.links
    .filter(l => l.type === 'POSITIONED_FOR')
    .forEach(l => {
      const brand = data.links.find(link => link.target === l.source && link.type === 'OFFERS')?.source;
      if (brand) {
        if (!audienceConnections[l.target]) audienceConnections[l.target] = [];
        audienceConnections[l.target].push(brand);
      }
    });
  
  // Find audiences with 2+ brands
  const sharedAudiences = Object.entries(audienceConnections)
    .filter(([_, brands]) => brands.length >= 2)
    .sort((a, b) => b[1].length - a[1].length);
  
  if (sharedAudiences.length > 0) {
    const [audienceId, brands] = sharedAudiences[0];
    const audienceNode = data.nodes.find(n => n.id === audienceId);
    const brandNodes = brands.map(bid => data.nodes.find(n => n.id === bid));
    
    return {
      type: 'competition',
      message: `${brandNodes[0]?.name} and ${brandNodes[1]?.name} compete for "${audienceNode?.name}"`,
      severity: 'high'
    };
  }
  
  return null;
};
// Show insight popup
const InsightPopup = ({insight}) => {
  if (!insight) return null;
  
  return (
    <div className="insight-popup">
      <span className="icon">💡</span>
      <p>{insight.message}</p>
      <button onClick={() => setShowDetails(true)}>Explore Competition</button>
    </div>
  );
};
Test: Popup appears with real competitive insight ✅

Testing Checklist
After migration, verify:

 Graph renders with 15 distinct colors
 Initial load shows ~60 nodes (not 2,697)
 "Show More" button reveals full dataset
 Node type legend displays correctly
 Hover tooltips show type-specific details
 Node type filters toggle visibility
 Inspector panel shows rich metadata
 Insight annotation appears on load
 Performance: loads in <3 seconds
 No console errors
Backward Compatibility
If your backend still returns old format:

Add adapter layer:

javascript
const adaptOldFormatToNew = (oldData) => {
  // Old format has just brand/topic
  // Keep it working until backend migrates
  return {
    nodes: oldData.nodes.map(n => ({
      ...n,
      // Add missing fields with defaults
      category: n.type === 'topic' ? 'General' : undefined,
      details: n.description || '',
      segmentType: n.type === 'topic' ? 'keyword' : undefined
    })),
    links: oldData.links.map(l => ({
      ...l,
      type: l.type || 'MENTIONS', // Add default relationship type
      value: l.weight
    }))
  };
};
// In fetch function
const data = await response.json();
const adaptedData = data.nodes[0]?.category ? data : adaptOldFormatToNew(data);
This lets you deploy frontend BEFORE backend is ready ✅

Summary
Minimal Changes Required:

Extend color mapping (5 min)
Add curated view filter (15 min)
Add node type legend (10 min)
Update tooltips (10 min)
Add type toggles (20 min)
Update inspector (30 min)
Add insight popup (30 min)
Total Time: ~2 hours for core functionality

Your existing graph library, layout, and most code stays the same! ✅

Next Steps:

Make these 7 changes
Test with production API
Deploy
Iterate based on user feedback