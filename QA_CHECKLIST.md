# Quality Assurance Checklist for Widget Development

## Pre-Development QA
- [ ] Understand the current data flow from database to UI
- [ ] Check what fields exist in the database (Neo4j/Supabase)
- [ ] Verify field names match between query and database schema
- [ ] Identify where real data lives vs. mock data

## Backend QA

### 1. Database Queries
- [ ] Test queries directly in database console first
- [ ] Verify field names are correct (e.g., `comment_count` not `comments`)
- [ ] Check if data needs to be joined from multiple sources
- [ ] Ensure queries return actual data, not null/empty

### 2. API Testing
```python
# Test individual query functions
from api.queries import network
result = network.get_key_voices()
print(f"Query returned {len(result)} records")
print(f"Sample data: {result[0] if result else 'No data'}")
```

### 3. Aggregator Testing
```python
# Test aggregator tier functions
from api.aggregator import _tier_network
data = _tier_network()
print(f"Keys in response: {data.keys()}")
```

## Frontend QA

### 1. Data Adapter Verification
- [ ] Check dashboardAdapter.ts correctly maps backend fields
- [ ] Ensure no mock data generation when real data exists
- [ ] Verify data transformations preserve actual values

### 2. Component Testing
- [ ] Inspect component props in React DevTools
- [ ] Check console for JavaScript errors
- [ ] Verify data binding in the template

## Integration QA

### 1. End-to-End Data Flow
```bash
# Test complete data flow
python3 -c "
from api.queries import network
import json

# Test backend
voices = network.get_key_voices()
print(f'Backend returns: {len(voices)} voices')
print(f'Sample: {voices[0] if voices else None}')

# Test aggregator
from api.aggregator import _tier_network
data = _tier_network()
print(f'Aggregator returns: {len(data.get('keyVoices', []))} voices')
"
```

### 2. Build and Deploy
```bash
# Build frontend
cd frontend && npm run build

# Check for build errors
echo $?  # Should be 0

# Test in browser
# - Check Network tab for API responses
# - Verify UI shows real data
```

## Common Issues and Fixes

### Issue 1: Shows placeholder data (User_123456)
**Cause:** Username/name fields don't exist in database
**Fix:**
- Check actual field names in database
- Join data from multiple tables if needed
- Fetch from Supabase if not in Neo4j

### Issue 2: All values show same percentage (1%)
**Cause:** Calculation uses wrong/null field
**Fix:**
- Verify field names match database schema
- Check for null values breaking calculations
- Add COALESCE for null handling

### Issue 3: Empty or "No data" widgets
**Cause:** Query returns no results
**Fix:**
- Test query directly in database
- Check WHERE clauses aren't too restrictive
- Verify relationships exist in graph

## Testing Commands

### Quick Backend Test
```bash
python3 -c "from api.queries import network; print('Voices:', len(network.get_key_voices()))"
python3 -c "from api.queries import network; print('Channels:', len(network.get_community_channels()))"
```

### Quick Frontend Test
```bash
cd frontend && npm run build && echo "Build successful"
```

## Final Verification
- [ ] Real usernames displayed (not User_ID)
- [ ] Actual engagement percentages (not all 1%)
- [ ] Topics from user's actual activity
- [ ] Channels where users are actually active
- [ ] Recommendations with evidence links
- [ ] Information velocity with real timestamps

## Documentation
- [ ] Document any schema assumptions
- [ ] Note data sources (Neo4j vs Supabase)
- [ ] List any fallback behaviors
- [ ] Include example API responses

---

## Summary of Today's Fixes

### Fixed Issues:
1. **Key Community Voices** - Now shows real usernames by fetching from Supabase
2. **Channel Engagement** - Fixed field name from `p.comments` to `p.comment_count`
3. **Information Velocity** - Added real temporal tracking with timestamps

### Key Learning:
Always perform QA testing after changes to ensure:
- Backend queries work correctly
- Data flows properly to frontend
- UI displays actual values, not mock data