/* Henkilöliikenne

YKR-ruudun asukkaiden liikkumisen kasvihuonekaasupäästöjen laskennan kulkumuodot jaetaan valtakunnallisen henkilöliikennetutkimuksen mukaisesti
jalankulkuun ja pyöräilyyn (jalkapyora), linja-autoihin (bussi), raideliikenteeseen (raide), henkilöautoihin (hlauto) ja muihin liikkumismuotoihin (muu).

Muiden liikkumismuotojen ryhmä sisältää taksi-, koulutaksi-, invataksi- ja kutsutaksimatkat sekä henkilöliikenteen matkat kuorma-autoilla,
mopoilla, moposkoottereilla, moottoripyörillä, kevytmoottoripyörillä, mopoautoilla, moottorikelkoilla, mönkijöillä, golfautoilla, traktoreilla,
työkoneilla, hälytysajoneuvoilla, moottori-, soutu- tai purjeveneellä, kanooteilla, kumiveneillä, jollilla, laivalla, autolautalla, pika-aluksella,
hevosella, koiravaljakolla tai muulla eläinkyydillä sekä suksilla, rullaluistimilla, rullasuksilla tai muulla liikunnallisella tavalla.

YKR-ruudussa asuvan väestön kulkumuotojen suoritteet apliikenne_hkm [hkm/a] ovat laskentavuonna

    apliikenne_hkm = v_yht * ap_kmuoto_hkmvrk * muunto_vuosi

YKR-ruudussa työssä käyvien henkilöiden kulkumuotojen suoritteet tpliikenne_hkm [hkm/a] saadaan kaavalla

    tpliikenne_hkm = tp_yht * tp_kmuoto_hkmvrk * muunto_vuosi 

Kulkumuotojen käyttövoimien suoriteosuuksilla painotetut keskikulutukset [kWh/km] ovat laskentavuonna

    kmuoto_kwhkm = kmuoto_kvoima_jakauma * kvoima_kwhkm

YKR-ruudun asukkaiden ja siellä työssäkäyvien kulkumuotojen laskentavuonna tarvitsemat energiamäärät [kWh/a] lasketaan kaavoilla

    apliikenne_kwh = apliikenne_hkm / apliikenne_kuormitus * kmuoto_kwhkm
    tpliikenne_kwh = tpliikenne_hkm / tpliikenne_kuormitus * kmuoto_kwhkm

Kulkumuotojen kasvihuonekaasupäästöjen keskimääräiset ominaispäästökertoimet [gCO2-ekv/kWh] määritellään käyttövoimien ominaispäästökertoimien suoriteosuuksilla painotettuna keskiarvona huomioiden samalla niiden bio-osuudet:

    kmuoto_gco2kwh = kmuoto_kvoima_jakauma * (sahko_gco2kwh * kvoima_apu1 + kvoima_foss_osa * kvoima_gco2kwh * kvoima_apu2)

YKR-ruudun asukkaiden ja ruudussa työssä käyvien liikkumiseen käyttämien eri kulkumuotojen aiheuttamat kasvihuonekaasupäästöt apliikenne_co2 ja tpliikenne_co2 [CO2-ekv/a] ovat laskentavuonna

    apliikenne_co2 = apliikenne_kwh * kmuoto_gco2kwh
    tpliikenne_co2 = tpliikenne_kwh * kmuoto_gco2kwh

*/

DROP FUNCTION IF EXISTS il_traffic_personal_co2;
CREATE OR REPLACE FUNCTION
public.il_traffic_personal_co2(
    v_yht integer, --   YKR-ruudun asukasmäärä laskentavuonna [as]. Lukuarvo riippuu laskentavuodesta.
    tp_yht integer, -- on YKR-ruudun työpaikkojen määrä laskentavuonna [as]. Lukuarvo riippuu laskentavuodesta.
    year integer, -- Vuosi, jonka perusteella päästöt lasketaan / viitearvot haetaan
    kulkumuoto varchar, 
    centdist integer,
    vyoh integer,
    area varchar,
    scenario varchar, -- PITKO:n mukainen kehitysskenaario
    gco2kwh_matrix real[], /* Ominaispäästökertoimien pohja */ /* Emission values template */
    baseYear integer default null,
    renew boolean default null
) 
RETURNS real AS
$$
DECLARE
    hlt_taulu1 varchar;
    tposuus real;
    kmuoto_km_muutos real;
    kmuoto_gco2kwh real[]; -- Kulkumuotojen kasvihuonekaasupäästöjen keskimääräiset ominaispäästökertoimet [gCO2-ekv/kWh] 
    
    kmuoto_hkmvrk real;
    kmuoto_kvoima_jakauma real[]; -- Kulkumuotojen käyttövoimajakaumaa. Arvot riippuvat taustaskenaariosta, laskentavuodesta, kulkumuodosta ja käyttövoimasta.
    kvoima_kwhkm real[]; -- Käyttövoimien energian keskikulutus [kWh/km]. Arvot riippuvat taustaskenaariosta, laskentavuodesta, kulkumuodosta ja käyttövoimasta.
    kmuoto_kvoima_mult_res real[];

    apliikenne_kuormitus real; -- Asukkaiden kulkumuotojen keskimääräiset kuormitukset laskentavuonna [hkm/km]. Arvo riippuu taustaskenaariosta, laskentavuodesta ja kulkumuodosta
    tpliikenne_kuormitus real; -- Työssä käyvien kulkumuotojen keskimääräiset kuormitukset laskentavuonna [hkm/km]. Arvo riippuu taustaskenaariosta, laskentavuodesta ja kulkumuodosta
    kmuoto_kwhkm real; -- Energian keskikulutus käyttövoimittain [kWh/km]. Lukuarvo riippuu taustaskenaariosta, laskentavuodesta, kulkumuodosta ja käyttövoimasta.
    apliikenne_hkm real; -- Asukkaiden kulkumuodoilla jalkapyora, bussi, raide, hlauto ja muu vuorokauden aikana tekemien matkojen keskimääräiset pituudet henkilökilometreinä [hkm/as/vrk]. Arvo riippuu laskentavuodesta, kulkumuodosta, yhdyskuntarakenteen vyöhykkeestä ja tarkastelualueesta.
    tpliikenne_hkm real; --  Ruudussa työssä käyvien kulkumuodoilla jalkapyora, bussi, raide, hlauto ja muu vuorokauden aikana tekemien matkojen keskimääräiset pituudet henkilökilometreinä [hkm/as/vrk]. Arvo riippuu laskentavuodesta, kulkumuodosta, yhdyskuntarakenteen vyöhykkeestä ja  ja tarkastelualueesta.
    hloliikenne_kwh real;
    hloliikenne_co2 real[];
BEGIN
    IF (v_yht <= 0 OR v_yht IS NULL) AND (tp_yht <= 0 OR tp_yht IS NULL) THEN
        RETURN 0;
    ELSE

    -------------------------------------------------------------------------------------------
    /* Matkat ja kuormitukset */

    EXECUTE 'SELECT hlt FROM aluejaot.alueet WHERE kunta = $1 OR maakunta = $1' INTO hlt_taulu1 USING area;

    EXECUTE 'SELECT ' || kulkumuoto || ' FROM liikenne.hlt_kmmuutos WHERE vyoh = $1' INTO kmuoto_km_muutos USING vyoh;
    EXECUTE 'SELECT ' || kulkumuoto || ' FROM liikenne.hlt_tposuus WHERE vyoh = $1' INTO tposuus USING vyoh;
    
    IF renew THEN
        EXECUTE 'CREATE TEMP TABLE hlt AS SELECT * FROM liikenne.'||hlt_taulu1||'';
        UPDATE hlt SET
        raide = (CASE
            WHEN hlt.vyoh = 1 THEN
                raide + bussi * 0.4 + ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00066 * (year - baseYear)) - raide - bussi)
            WHEN hlt.vyoh = 2 THEN          
                raide + bussi * 0.3 + ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00033 * (year - baseYear)) - raide - bussi)
            WHEN hlt.vyoh = 3 THEN 
                raide + bussi * 0.20 + ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00012 * (year - baseYear)) - raide - bussi)
            WHEN hlt.vyoh = 4 OR hlt.vyoh = 10 THEN
                raide + bussi * 0.15 + ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00012 * (year - baseYear)) - raide - bussi)
            WHEN hlt.vyoh = 837101 OR hlt.vyoh = 9993 THEN 
                raide + bussi * 0.5 + ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00066 * (year - baseYear)) - raide - bussi)
            ELSE raide
        END),
        bussi = (CASE
            WHEN hlt.vyoh = 1 THEN
                bussi * 0.6
            WHEN hlt.vyoh = 2 THEN          
                bussi * 0.7
            WHEN hlt.vyoh = 3 THEN 
                bussi * 0.80
            WHEN hlt.vyoh = 4 OR hlt.vyoh = 10 THEN
                bussi * 0.95
            WHEN hlt.vyoh = 837101 OR hlt.vyoh = 9993 THEN 
                bussi * 0.5
            ELSE bussi
        END),
        hlauto = (CASE
            WHEN hlt.vyoh = 1 THEN
                hlauto - ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00066 * (year - baseYear)) - raide - bussi)
            WHEN hlt.vyoh = 2 THEN          
                hlauto - ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00033 * (year - baseYear)) - raide - bussi)
            WHEN hlt.vyoh = 3 THEN 
                hlauto - ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00012 * (year - baseYear)) - raide - bussi)
            WHEN hlt.vyoh = 4 OR hlt.vyoh = 10 THEN
                hlauto - ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00012 * (year - baseYear)) - raide - bussi)
            WHEN hlt.vyoh = 837101 OR hlt.vyoh = 9993 THEN 
                hlauto - ((jalkapyora + bussi + raide + hlauto + muu) * ((raide +  bussi) / (jalkapyora + bussi + raide + hlauto + muu) + 0.00066 * (year - baseYear)) - raide - bussi)
            ELSE hlauto
        END);
        EXECUTE 'SELECT ' || kulkumuoto || ' FROM hlt WHERE vyoh = $1' INTO kmuoto_hkmvrk USING vyoh;
        DROP TABLE IF EXISTS hlt;
    ELSE 
        EXECUTE 'SELECT ' || kulkumuoto || ' FROM liikenne.'||hlt_taulu1||' WHERE vyoh = $1' INTO kmuoto_hkmvrk USING vyoh;
    END IF;

    kmuoto_hkmvrk := (CASE
    WHEN centdist > 2 AND centdist < 10 AND vyoh != 4 
        THEN kmuoto_hkmvrk + COALESCE((centdist - 2) * kmuoto_km_muutos, 0)
    WHEN centdist > 2 AND vyoh = 4
        THEN(CASE WHEN kmuoto_hkmvrk - COALESCE((centdist - 2) * kmuoto_km_muutos, 0) > 0 THEN kmuoto_hkmvrk - COALESCE((centdist - 2) * kmuoto_km_muutos, 0) ELSE kmuoto_hkmvrk END)
    ELSE kmuoto_hkmvrk END);

    /* muunto vuorokausisuoritteesta vuositasolle [vrk/a] (365) */
    SELECT v_yht * (1-tposuus) * 365 * kmuoto_hkmvrk INTO apliikenne_hkm;
    SELECT tp_yht * tposuus * 365 * kmuoto_hkmvrk INTO tpliikenne_hkm; -- Tilastojen mukaan tehollisia työpäiviä keskimäärin noin 228-230 per vuosi

    EXECUTE 'SELECT ' || kulkumuoto || ' FROM liikenne.apliikenne_kuormitus WHERE skenaario = $1 AND vuosi = $2'
        INTO apliikenne_kuormitus USING scenario, year;
    EXECUTE 'SELECT ' || kulkumuoto || ' FROM liikenne.tpliikenne_kuormitus WHERE skenaario = $1 AND vuosi = $2'
        INTO tpliikenne_kuormitus USING scenario, year;

    -------------------------------------------------------------------------------------------
    /* Kulkumuotojen käyttövoimien suoriteosuuksilla painotetut keskikulutukset */
    SELECT array[kvoima_bensiini, kvoima_etanoli, kvoima_diesel, kvoima_kaasu, kvoima_phev_b, kvoima_phev_d, kvoima_ev, kvoima_vety]
        INTO kmuoto_kvoima_jakauma
        FROM liikenne.kvoima_kmuoto_jakauma
        WHERE vuosi = year AND skenaario = scenario AND kmuoto = kulkumuoto;
    SELECT array[kvoima_bensiini, kvoima_etanoli, kvoima_diesel, kvoima_kaasu, kvoima_phev_b, kvoima_phev_d, kvoima_ev, kvoima_vety]
        INTO kvoima_kwhkm
        FROM liikenne.kvoima_kwhkm
        WHERE vuosi = year AND skenaario = scenario AND kmuoto = kulkumuoto;

    SELECT array(SELECT unnest(kmuoto_kvoima_jakauma) * unnest(kvoima_kwhkm)) INTO kmuoto_kvoima_mult_res;
    SELECT array(SELECT unnest(kmuoto_kvoima_jakauma) * unnest(gco2kwh_matrix)) INTO kmuoto_gco2kwh;

    kmuoto_kwhkm := SUM(a) FROM unnest(kmuoto_kvoima_mult_res) a;
   
    -------------------------------------------------------------------------------------------
    /* KWh-laskenta */
    /* KWh calculations */

    SELECT kmuoto_kwhkm * (COALESCE(apliikenne_hkm,0) / apliikenne_kuormitus + COALESCE(tpliikenne_hkm,0) / tpliikenne_kuormitus) INTO hloliikenne_kwh;

    -------------------------------------------------------------------------------------------

    SELECT array(SELECT hloliikenne_kwh * unnest(kmuoto_gco2kwh)) INTO hloliikenne_co2;
        RETURN SUM(a) FROM unnest(hloliikenne_co2) a;
    END IF;
END;
$$ LANGUAGE plpgsql;