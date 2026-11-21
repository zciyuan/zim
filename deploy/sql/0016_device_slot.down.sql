DROP INDEX IF EXISTS uk_zim_device_slot;
ALTER TABLE zim_device DROP CONSTRAINT IF EXISTS zim_device_pkey;
ALTER TABLE zim_device ADD PRIMARY KEY (owner_id, platform);
ALTER TABLE zim_device DROP COLUMN IF EXISTS slot;
