# Inventory Consolidation to Latest Revisions Procedure

**Created:** 2025-01-10 15:00  
**Author:** Sean McCarthy  
**GitHub Issue:** https://github.com/ijack-technologies/rcom/issues/1106

## Overview

This document describes the automatic consolidation of warehouse inventory quantities and management settings from older part revisions to the latest revision during BOM Master upload.

## Problem Statement

When new part revisions are added (e.g., part 'ABC' changes from r0 to r1), inventory quantities remain with the old revision. This causes several issues:

1. Users must manually track which revision has actual inventory
2. Reports show inventory spread across multiple revisions
3. New revisions appear to have zero inventory even when older revisions have stock
4. Reserved quantities remain with old revisions, affecting availability calculations
5. Warehouse management settings (min/max stock, reorder points) are lost with new revisions
6. Confusion about which revision to use for orders and work orders

## Solution

The `consolidate_inventory_to_latest_revisions()` function automatically:

1. Identifies part families with multiple active revisions
2. Finds the latest revision for each part family
3. Sums all inventory (quantity, quantity_reserved, quantity_desired) from older revisions
4. Transfers the totals to the newest revision
5. Copies warehouse management settings from the previous latest revision:
   - `warehouse_min_stock`, `warehouse_max_stock`
   - `warehouse_reorder_point`, `warehouse_reorder_quantity`
   - `safety_stock`, `lead_time_days`
   - `avg_daily_usage`, `cycle_count_frequency`
6. Sets older revisions to zero for all quantity fields
7. Maintains total inventory integrity and configuration consistency

## Technical Implementation

### Function Details

```python
def consolidate_inventory_to_latest_revisions(conn: psycopg2.extensions.connection) -> None
```

**Location:** `/workspace/project/upload_bom_master_parts_to_db.py:1309`

### Algorithm

1. **Identify Multi-Revision Parts**
   - Groups parts by `part_name` (computed field without revision)
   - Finds families with COUNT > 1
   - Identifies latest revision using MAX(part_rev)

2. **Transfer Process**
   - For each part family with multiple revisions:
     - Get all older revision part IDs
     - For each warehouse containing these parts:
       - Copy warehouse management settings from previous latest revision
       - Sum quantities from older revisions
       - Add to latest revision (update or insert)
       - Set older revisions to zero

3. **Integrity Verification**
   - Verifies total quantities remain unchanged
   - Logs detailed transfer information
   - Reports consolidation summary

### Example Scenario

Before consolidation:
```
Part ABC r0: 
  - Warehouse 1 = 3 units (1 reserved), 2 desired, min_stock=10, reorder_point=25
  - Warehouse 2 = 2 units (0 reserved), 1 desired, min_stock=15, reorder_point=30
Part ABC r1: 
  - Warehouse 1 = 0 units (0 reserved), 0 desired, no warehouse config
  - Warehouse 2 = 0 units (0 reserved), 0 desired, no warehouse config
```

After consolidation:
```
Part ABC r0: 
  - Warehouse 1 = 0 units (0 reserved), 0 desired
  - Warehouse 2 = 0 units (0 reserved), 0 desired
Part ABC r1: 
  - Warehouse 1 = 3 units (1 reserved), 2 desired, min_stock=10, reorder_point=25
  - Warehouse 2 = 2 units (0 reserved), 1 desired, min_stock=15, reorder_point=30
```

Total inventory remains: 5 units actual, 1 unit reserved, 3 units desired - all transferred to the latest revision with warehouse settings preserved.

### Database Tables Affected

- **parts**: Read to identify revisions (no modifications)
- **warehouses_parts_rel**: 
  - Updates quantity, quantity_reserved, and quantity_desired for latest revisions
  - Sets quantity=0, quantity_reserved=0, quantity_desired=0 for older revisions
  - May insert new records for latest revision if not exists

### Integration Point

Called in `entrypoint()` function after:
- Parts uploaded to database
- Before initializing parts in warehouses

This ensures:
1. Latest revisions get the consolidated inventory
2. New warehouse-part initialization doesn't create duplicate records

## Testing

### Test Script

Run the test script to validate consolidation:
```bash
python /workspace/test_consolidate_revisions.py
```

The script will:
1. Find parts with multiple revisions
2. Show inventory distribution before consolidation
3. Run the consolidation function
4. Verify inventory transferred correctly
5. Confirm older revisions have zero quantity

### Verification Queries

Find parts with multiple revisions:
```sql
SELECT 
    part_name,
    COUNT(*) as revision_count,
    STRING_AGG(part_num || ' (r' || part_rev::int || ')', ', ' ORDER BY part_rev) as all_revisions
FROM public.parts
WHERE is_active = true 
AND (flagged_for_deletion IS NULL OR flagged_for_deletion = false)
GROUP BY part_name
HAVING COUNT(*) > 1
ORDER BY revision_count DESC;
```

Check inventory distribution for a part family:
```sql
SELECT 
    p.part_num,
    p.part_rev,
    w.name as warehouse,
    wpr.quantity,
    wpr.quantity_reserved,
    wpr.quantity_desired
FROM public.parts p
LEFT JOIN public.warehouses_parts_rel wpr ON wpr.part_id = p.id
LEFT JOIN public.warehouses w ON w.id = wpr.warehouse_id
WHERE p.part_name = 'YOUR_PART_NAME_HERE'
AND p.is_active = true
ORDER BY p.part_rev DESC, w.name;
```

Verify no older revisions have inventory:
```sql
SELECT 
    p.part_name,
    p.part_num,
    p.part_rev,
    SUM(wpr.quantity) as total_quantity,
    SUM(wpr.quantity_reserved) as total_reserved,
    SUM(wpr.quantity_desired) as total_desired
FROM public.parts p
JOIN public.warehouses_parts_rel wpr ON wpr.part_id = p.id
WHERE p.is_active = true
AND p.part_rev < (
    SELECT MAX(part_rev) 
    FROM public.parts p2 
    WHERE p2.part_name = p.part_name 
    AND p2.is_active = true
)
AND (wpr.quantity > 0 OR wpr.quantity_reserved > 0 OR wpr.quantity_desired > 0)
GROUP BY p.part_name, p.part_num, p.part_rev;
```

## Performance Considerations

- Processes only parts with multiple revisions (typically < 10% of parts)
- Uses efficient SQL with CTEs and bulk updates
- Transactional to ensure data integrity
- Typically completes in seconds even for large datasets

## Manual Consolidation

If needed to run consolidation manually:
```python
from project.upload_bom_master_parts_to_db import consolidate_inventory_to_latest_revisions
from project.utils import get_conn

with get_conn(db="aws_rds") as conn:
    consolidate_inventory_to_latest_revisions(conn)
```

## Rollback Procedure

If consolidation needs to be reversed (not recommended):
1. Identify the consolidation timestamp from logs
2. Use audit trails to determine original quantities
3. Manually redistribute quantities to older revisions
4. Note: This is complex and should be avoided

## Future Enhancements

1. Add configuration to exclude specific part families from consolidation
2. Create audit log table to track all consolidations
3. Add email notifications for large quantity transfers
4. Consider archiving older revisions after consolidation
5. Add option to consolidate only specific part categories