ALTER TABLE zim_device ADD COLUMN IF NOT EXISTS slot SMALLINT;
WITH ranked AS (
  SELECT owner_id, platform, row_number() OVER (PARTITION BY owner_id ORDER BY platform) AS rn
  FROM zim_device WHERE slot IS NULL
)
UPDATE zim_device d SET slot = ranked.rn FROM ranked
WHERE d.owner_id = ranked.owner_id AND d.platform = ranked.platform AND d.slot IS NULL;
UPDATE zim_device SET slot = 1 WHERE slot IS NULL;
ALTER TABLE zim_device ALTER COLUMN slot SET NOT NULL;
UPDATE zim_device SET device_id = 'legacy-' || owner_id || '-' || platform WHERE device_id = '';
ALTER TABLE zim_device DROP CONSTRAINT IF EXISTS zim_device_pkey;
ALTER TABLE zim_device ADD PRIMARY KEY (owner_id, device_id);
CREATE UNIQUE INDEX IF NOT EXISTS uk_zim_device_slot ON zim_device (owner_id, slot);
