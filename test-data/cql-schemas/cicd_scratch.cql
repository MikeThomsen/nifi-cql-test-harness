-- Scratch table for the ExecuteCQL insert/update/delete specs in cicd-specs. Nothing else reads or
-- writes it, so those specs can create and remove rows without disturbing the message data the
-- throughput specs count.
CREATE TABLE IF NOT EXISTS nifi_test.cicd_scratch (
    item_id    text,
    name       text,
    quantity   int,
    updated_at timestamp,
    PRIMARY KEY (item_id)
);
