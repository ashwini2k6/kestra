ALTER TABLE triggers add "vnode" INTEGER GENERATED ALWAYS AS ((value ->> 'vnode')::integer) STORED;
CREATE INDEX IF NOT EXISTS idx_triggers_vnode ON triggers (vnode);

ALTER TYPE queue_type ADD VALUE IF NOT EXISTS 'io.kestra.scheduler.events.TriggerEvent';