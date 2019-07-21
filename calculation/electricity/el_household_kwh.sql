-- Kotitaloudet

-- LASKENTAKAAVOJEN TULOSTEN KOOSTAMINEN, PARAMETRIEN POIMINTA TAULUISTA JA LASKENTAKAAVOJEN KUTSUT

/* Rakennustyypeittäiset kotitalouksien sähkönkulutukset */
/* Esimerkkikutsu: SELECT il_sahko_koti_co2(erpien_ala, 'erpien', 2019,'wem', 'em','hankinta') */ 
DROP FUNCTION IF EXISTS il_el_household_kwh;
CREATE OR REPLACE FUNCTION
public.il_el_household_kwh(
    ala_or_vaesto real,
    year integer,
    rakennustyyppi varchar,
    scenario varchar,
    sahko_as real default null
)
RETURNS real AS
$$
DECLARE
    sahko_koti_laite real;
    sahko_koti_valo real;
BEGIN
    IF ala_or_vaesto <= 0 OR ala_or_vaesto IS NULL THEN
        RETURN 0;
    ELSE
        IF rakennustyyppi IS NOT NULL THEN 
            EXECUTE 'SELECT ' || rakennustyyppi || ' FROM rakymp.sahko_koti_laite WHERE skenaario = $1 AND vuosi = $2'
                INTO sahko_koti_laite USING scenario, year;
            EXECUTE 'SELECT ' || rakennustyyppi || ' FROM rakymp.sahko_koti_valo WHERE skenaario = $1 AND vuosi = $2'
                INTO sahko_koti_valo USING scenario, year;
            RETURN ala_or_vaesto * (sahko_koti_laite + sahko_koti_valo); -- sahko_koti_kwh
        ELSE
            RETURN ala_or_vaesto * sahko_as; -- sahko_koti_as_kwh
        END IF;
    END IF;
END;
$$ LANGUAGE plpgsql;