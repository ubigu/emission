-- Tavaraliikenne

-- LASKENTAKAAVOJEN TULOSTEN KOOSTAMINEN, PARAMETRIEN POIMINTA TAULUISTA JA LASKENTAKAAVOJEN KUTSUT

-- LASKENTAKAAVAT
DROP FUNCTION IF EXISTS il_traffic_iwhs_co2;
CREATE OR REPLACE FUNCTION
public.il_traffic_iwhs_co2(
    rak_ala_lkm integer, -- Rakennusten kerrosala tai lukumäärä (vain teoll ja varast - tapauksissa)
    year integer, -- Vuosi, jonka perusteella päästöt lasketaan / viitearvot haetaan
    rakennustyyppi varchar, -- Rakennustyyppi | Building type. esim. | e.g. 'erpien', 'rivita'
    kulkumuoto varchar, -- Kulkumuoto
    scenario varchar, -- PITKO:n mukainen kehitysskenaario
    gco2kwh_matrix real[] /* Ominaispäästökertoimien pohja */ /* Emission values template */
)
RETURNS real AS
$$
DECLARE
    ptv_suorite real;
    ptv_kuljetus_km real;
    ptv_kuormitus real;
    ptv_liikenne_kwh real;
    ptv_liikenne_co2 real[];
    kmuoto_kwhkm real;
    kmuoto_kvoima_jakauma real[];
    kvoima_kwhkm real[];
    kmuoto_kvoima_mult_res real[];
    kmuoto_gco2kwh real[];
    workdays real default 260;
BEGIN
    IF rak_ala_lkm <= 0 OR rak_ala_lkm IS NULL THEN
        RETURN 0;
    ELSE

        EXECUTE 'SELECT ' || rakennustyyppi || '::real FROM liikenne.t_suorite WHERE vuosi = $1 AND skenaario = $2 AND kmuoto = $3'
            INTO ptv_suorite USING year, scenario, kulkumuoto;
        EXECUTE 'SELECT ' || rakennustyyppi || ' FROM liikenne.t_kuljetus_km WHERE vuosi = $1 AND skenaario = $2 AND kmuoto = $3'
            INTO ptv_kuljetus_km USING year, scenario, kulkumuoto;
        EXECUTE 'SELECT ' || kulkumuoto || ' FROM liikenne.tliikenne_kuormitus WHERE skenaario = $1 AND vuosi = $2'
            INTO ptv_kuormitus USING scenario, year;

        -------------------------------------------------------------------------------------------
        /* Kulkumuotojen käyttövoimien suoriteosuuksilla painotetut keskikulutukset */

        SELECT array[kvoima_bensiini, kvoima_etanoli, kvoima_diesel, kvoima_kaasu, kvoima_phev_b, kvoima_phev_d, kvoima_ev, kvoima_vety]
            INTO kmuoto_kvoima_jakauma FROM liikenne.kvoima_kmuoto_jakauma
            WHERE vuosi = year AND skenaario = scenario AND kmuoto = kulkumuoto;
        SELECT array[kvoima_bensiini, kvoima_etanoli, kvoima_diesel, kvoima_kaasu, kvoima_phev_b, kvoima_phev_d, kvoima_ev, kvoima_vety]
            INTO kvoima_kwhkm
            FROM liikenne.kvoima_kwhkm
            WHERE vuosi = year AND skenaario = scenario AND kmuoto = kulkumuoto;

        SELECT array(SELECT unnest(kmuoto_kvoima_jakauma) * unnest(kvoima_kwhkm)) INTO kmuoto_kvoima_mult_res;
        SELECT array(SELECT unnest(kmuoto_kvoima_jakauma) * unnest(gco2kwh_matrix)) INTO kmuoto_gco2kwh;

        kmuoto_kwhkm := SUM(a) FROM unnest(kmuoto_kvoima_mult_res) a;
    
        -------------------------------------------------------------------------------------------

        SELECT (CASE WHEN rakennustyyppi NOT IN ('teoll', 'varast') THEN rak_ala_lkm * 0.01 ELSE rak_ala_lkm END) * ptv_suorite * ptv_kuljetus_km * ptv_kuormitus * kmuoto_kwhkm INTO ptv_liikenne_kwh;

        SELECT array(SELECT ptv_liikenne_kwh * unnest(kmuoto_gco2kwh)) INTO ptv_liikenne_co2;
        RETURN SUM(a) * 260 FROM unnest(ptv_liikenne_co2) a;

    END IF;
END;
$$ LANGUAGE plpgsql;