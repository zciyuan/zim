-- Sequential account ids.
--
-- Device keys are encoded as uid*100+platform (ADR-0014, currently shipped form),
-- which overflows int64 for snowflake uids (~2.5e17). Registration therefore
-- allocates small sequential ids from this sequence; message/group ids keep
-- using the snowflake generator.
CREATE SEQUENCE IF NOT EXISTS zim_user_id_seq
    AS BIGINT
    START WITH 20000
    INCREMENT BY 1
    NO MAXVALUE
    CACHE 50;
