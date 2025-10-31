# Warehouse Parts Initialization Procedure

**Created:** 2025-01-10 14:30  
**Author:** Sean McCarthy  
**GitHub Issue:** https://github.com/ijack-technologies/rcom/issues/1105

## Overview

This document describes the automatic initialization of parts in all active warehouses with zero quantity after BOM Master upload.

## Problem Statement

When new parts are added from the BOM Master spreadsheet to the `parts` table, they don't automatically appear in the warehouse inventory system. Users searching for these new parts in various warehouses see "no records found" instead of "0 inventory", which creates confusion about whether the part exists in the system.

## Solution

A new function `initialize_parts_in_warehouses()` has been added to `upload_bom_master_parts_to_db.py` that:

1. Queries all active warehouses
2. Queries all active parts (not flagged for deletion)
3. Creates warehouse-part relationships with zero quantity where they don't exist
4. Uses efficient bulk insert with `ON CONFLICT DO NOTHING` to avoid duplicates

## Technical Implementation

### Function Details

```python
def initialize_parts_in_warehouses(conn: psycopg2.extensions.connection) -> None
```

**Location:** `/workspace/project/upload_bom_master_parts_to_db.py:1309`

### Key Features

1. **Efficient Bulk Insert**: Uses `psycopg2.extras.execute_values` for batch processing
2. **Conflict Handling**: Uses `ON CONFLICT DO NOTHING` to skip existing relationships
3. **Progress Logging**: Reports statistics before and after initialization
4. **Batch Processing**: Processes records in batches of 1,000 to avoid memory issues

### Database Tables Affected

- **warehouses_parts_rel**: Junction table that tracks inventory quantities
  - Fields initialized: `quantity=0`, `quantity_reserved=0`, `quantity_desired=0`, `average_cost=0`, `last_cost=0`
  - Only creates records where `(warehouse_id, part_id)` combination doesn't exist

### Integration Point

The function is called in the `entrypoint()` function after:
- Parts have been uploaded to the database
- Finished goods relationships have been updated
- Unused parts have been marked/deleted

## Testing

### Manual Test Procedure

1. Run the test script:
   ```bash
   python /workspace/test_warehouse_parts_init.py
   ```

2. The script will:
   - Show current statistics (warehouses, parts, relationships)
   - Run the initialization function
   - Verify all parts exist in all warehouses
   - Display sample newly created records

### Verification Queries

Check parts without warehouse records:
```sql
SELECT p.part_num, p.description
FROM public.parts p
WHERE p.is_active = true 
AND (p.flagged_for_deletion IS NULL OR p.flagged_for_deletion = false)
AND NOT EXISTS (
    SELECT 1 
    FROM public.warehouses_parts_rel wpr
    JOIN public.warehouses w ON w.id = wpr.warehouse_id
    WHERE wpr.part_id = p.id
    AND w.is_active = true
);
```

Count relationships by warehouse:
```sql
SELECT w.name, COUNT(*) as part_count
FROM public.warehouses_parts_rel wpr
JOIN public.warehouses w ON w.id = wpr.warehouse_id
JOIN public.parts p ON p.id = wpr.part_id
WHERE w.is_active = true 
AND p.is_active = true
AND (p.flagged_for_deletion IS NULL OR p.flagged_for_deletion = false)
GROUP BY w.name
ORDER BY w.name;
```

## Performance Considerations

- For 3 warehouses and 5,000 parts = 15,000 potential relationships
- Bulk insert processes in batches to avoid memory issues
- Uses `ON CONFLICT DO NOTHING` for optimal performance
- Typically completes in under 30 seconds for standard datasets

## Rollback Procedure

If needed, remove zero-quantity records created by this process:
```sql
DELETE FROM public.warehouses_parts_rel
WHERE quantity = 0
AND quantity_reserved = 0
AND quantity_desired = 0
AND average_cost = 0
AND last_cost = 0
AND timestamp_utc_inserted > '2025-01-10'::date;
```

## Future Enhancements

1. Add configuration option to skip initialization
2. Allow selective initialization (specific warehouses or part categories)
3. Add email notification summary after initialization
4. Consider background job processing for very large datasets