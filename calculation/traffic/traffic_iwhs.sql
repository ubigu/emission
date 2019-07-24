/* Tavaraliikenne | Goods traffic

Palvelurakennukset luokitellaan kasvihuonekaasupäästölaskelmissa myymälärakennuksiin (myymal), majoitusliikerakennuksiin (majoit),
asuntolarakennuksiin (asla), ravintoloihin ja ruokaloihin (ravint), toimistorakennuksiin (tsto), liikenteen rakennuksiin (liiken),
hoitoalan rakennuksiin (hoito), kookoontumisrakennuksiin (kokoon), opetusrakennuksiin (opetus) ja muihin rakennuksiin (muut).
Teollisuus- ja varastorakennuksiin sisältyvät teollisuusrakennukset (teoll) ja varastorakennukset (varast).

YKR-ruuudun palvelurakennustyyppien paketti- ja kuorma-autojen tavarakuljetusten vuosisuoritteet palv_km [km/a] ovat laskentavuonna

    ptv_km = rakennus_ala * muunto_ala_tliikenne * palv_suorite * palv_kuljetus_km * arkipaivat

Teollisuus- ja varastorakennusten paketti- tai kuorma-autojen tavarakuljetusten vuosisuoritteiden teoll_km [km/a] laskentaan käytetään
kerrosalan sijaan rakennustyypin lukumääriä ja tavaraliikenteen käyntikertojen määrää vuorokauden aikana:

    ptv_km = rakennus_lkm * teoll_suorite * teoll_kuljetus_km * arkipaivat

Paketti- ja kuorma-autojen käyttövoimien suoriteosuuksilla painotetut keskikulutukset kmuoto_kwhkm [kWh/km] lasketaan samalla kaavalla kuin henkilöliikenteen tapauksessa eli

    kmuoto_kwhkm = kmuoto_kvoima_jakauma * kvoima_kwhkm

Palvelurakennusten ja muiden rakennusten sekä teollisuus- ja varastorakennusten paketti- ja kuorma-autoilla tehdyn tavaraliikenteen vuosittainen energian käyttö [kWh/a] lasketaan kaavoilla

    ptv_liikenne_kwh = ptv_km * kmuoto_kwhkm * ptv_kuormitus

Laskentavuoden palvelu- ja teollisuusrakennusten paketti- ja kuorma-autojen tavarakuljetussuoritteiden aiheuttamat kasvihuonekaasupäästöt [CO2-ekv/a] ovat

    ptv_liikenne_co2 = ptvliikenne_palv_kwh * kmuoto_gco2kwh

*/

DROP FUNCTION IF EXISTS il_traffic_iwhs_co2;
CREATE OR REPLACE FUNCTION
public.il_traffic_iwhs_co2(
    rak_ala_lkm integer, -- Rakennusten kerrosala tai lukumäärä (vain teoll ja varast - tapauksissa)
    year integer, -- Vuosi, jonka perusteella päästöt lasketaan / viitearvot haetaan
    rakennustyyppi varchar, -- Rakennustyyppi | Building type. esim. | e.g. 'erpien', 'rivita'
    kulkumuoto varchar, -- Kulkumuoto
    scenario varchar, -- PITKO:n mukainen kehitysskenaario
    gco2kwh_matrix real[] /* Ominaispäästökertoimien pohja | Emission values template */
)
RETURNS real AS
$$
DECLARE
    ptv_suorite real; -- Palvelu [krt/100 m2,vrk], teollisuus- tai varastorakennuksen [krt/kpl,vrk] paketti- ja kuorma-autokuljetusten kiinteistöä kohti laskettujen käyntikertojen lukumäärä vuorokauden aikana. Lukuarvo riippuu taustaskenaariosta, laskentavuodesta ja rakennuksen tyypistä.
    ptv_kuljetus_km real; -- Palvelu-, teollisuus- tai varastorakennuksen paketti- ja kuorma-autokäyntien pauto ja kauto keskimääräiset pituudet [km/krt]. Lukuarvo riippuu laskentavuodesta ja rakennuksen tyypistä.
    ptv_kuormitus real; -- Paketti- ja kuorma-autoilla tehtyjen käyntien keskimääräinen kuormausaste [ei yksikköä]. Riippuu taustaskenaariosta, laskentavuodesta ja kulkumuodosta.
    ptv_liikenne_kwh real;
    ptv_liikenne_co2 real[];
    kmuoto_kwhkm real;
    kmuoto_kvoima_jakauma real[]; -- Käyttövoimajakaumaa kulkumuodoittain. Lukuarvo riippuu taustaskenaariosta, laskentavuodesta, kulkumuodosta ja käyttövoimasta.
    kvoima_kwhkm real[]; -- Energian keskikulutus käyttövoimittain [kWh/km]. Lukuarvo riippuu taustaskenaariosta, laskentavuodesta, kulkumuodosta ja käyttövoimasta.
    kmuoto_kvoima_mult_res real[];
    kmuoto_gco2kwh real[];
    workdays real default 260; -- Arkipäivien lukumäärä vuodessa (260) [vrk/a].
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

        /* Muunto_ala_tliikenne [m2/m2] muuntaa kerrosneliömetrit sadoiksi kerrosneliömetreiksi (0.01) */
        SELECT (CASE WHEN rakennustyyppi NOT IN ('teoll', 'varast') THEN rak_ala_lkm * 0.01 ELSE rak_ala_lkm END) * ptv_suorite * ptv_kuljetus_km * ptv_kuormitus * kmuoto_kwhkm INTO ptv_liikenne_kwh;

        SELECT array(SELECT ptv_liikenne_kwh * unnest(kmuoto_gco2kwh)) INTO ptv_liikenne_co2;
        RETURN SUM(a) * 260 FROM unnest(ptv_liikenne_co2) a;

    END IF;
END;
$$ LANGUAGE plpgsql;