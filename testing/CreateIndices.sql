-- Traffic
CREATE INDEX IF NOT EXISTS personal_traffic_index ON traffic.citizen_traffic_stress (mun, scenario, year);
CREATE INDEX IF NOT EXISTS industry_performance_index ON traffic.industr_performance (mun, scenario, year);
CREATE INDEX IF NOT EXISTS industry_performance_km_index ON traffic.industr_transport_km (mun, scenario, year);
CREATE INDEX IF NOT EXISTS mode_power_distribution_index ON traffic.mode_power_distribution (mun, scenario, year);
CREATE INDEX IF NOT EXISTS power_fossil_share_index ON traffic.power_fossil_share (scenario, year);
CREATE INDEX IF NOT EXISTS power_gco2kwh_index ON traffic.power_gco2kwh (scenario, year);
CREATE INDEX IF NOT EXISTS power_kwhkm_index ON traffic.power_kwhkm (mun, scenario, year);
CREATE INDEX IF NOT EXISTS service_performance_index ON traffic.service_performance (mun, scenario, year);
CREATE INDEX IF NOT EXISTS service_transport_index ON traffic.services_transport_km (mun, scenario, year);
CREATE INDEX IF NOT EXISTS workers_traffic_stress ON traffic.workers_traffic_stress (mun, scenario, year);
-- built
CREATE INDEX IF NOT EXISTS build_demolish_index ON built.build_demolish_energy_gco2m2 (scenario, year);
CREATE INDEX IF NOT EXISTS build_materia_index ON built.build_materia_gco2m2 (mun, scenario, year);
CREATE INDEX IF NOT EXISTS build_rebuilding_energy_index ON built.build_rebuilding_energy_gco2m2 (mun, scenario, year);
CREATE INDEX IF NOT EXISTS build_rebuilding_share ON built.build_rebuilding_share (mun, scenario, year);
CREATE INDEX IF NOT EXISTS build_renovation_energy_index ON built.build_renovation_energy_gco2m2 (mun, scenario, year);
CREATE INDEX IF NOT EXISTS construction_index ON built.constr_new_build_energy_gco2m2 (mun, scenario, year);
CREATE INDEX IF NOT EXISTS cooling_change_index ON built.cooling_change (mun, scenario, year);
CREATE INDEX IF NOT EXISTS cooling_proportions_index ON built.cooling_proportions_kwhm2 (mun, scenario, rakv);
CREATE INDEX IF NOT EXISTS distribution_heating_system_index ON built.distribution_heating_systems (scenario, year, rakv, rakennus_tyyppi);

CREATE INDEX IF NOT EXISTS electricity_home_device_index ON built.electricity_home_device (mun, scenario, year);
CREATE INDEX IF NOT EXISTS electricity_home_light_index ON built.electricity_home_light (mun, scenario, year);
CREATE INDEX IF NOT EXISTS electricity_industry_index ON built.electricity_industry_kwhm2 (mun, scenario, year);
CREATE INDEX IF NOT EXISTS electricity_property_change_index ON built.electricity_property_change (mun, scenario, year);
CREATE INDEX IF NOT EXISTS electricity_property_kwhkm_index ON built.electricity_property_kwhm2 (mun, scenario, rakv);
CREATE INDEX IF NOT EXISTS electricity_service_index ON built.electricity_service_kwhm2 (mun, scenario, year);
CREATE INDEX IF NOT EXISTS electricity_warehouse_index ON built.electricity_warehouse_kwhm2 (mun, scenario, year);

CREATE INDEX IF NOT EXISTS spaces_efficiency_index ON built.spaces_efficiency (mun, scenario, rakv, rakennus_tyyppi);
CREATE INDEX IF NOT EXISTS spaces_kwhm2_index ON built.spaces_kwhm2 (mun, scenario, rakv, year);
CREATE INDEX IF NOT EXISTS water_kwhm2_index ON built.water_kwhm2 (mun, scenario, rakv, rakennus_tyyppi);