import requests

continent_cache = {}

fallback_mapping = {
    "EIRE": "Ireland",
    "Channel Islands": "Guernsey",
    "European Community": "France",
    "Unspecified": None,
}

def get_continent_from_country(country_name):
    if not country_name or country_name.strip() == "":
        return None

    # Vérifie dans le cache
    if country_name in continent_cache:
        return continent_cache[country_name]

    try:
        res = requests.get(f"https://restcountries.com/v3.1/name/{country_name}", timeout=5)
        if res.status_code == 200:
            print("call_api_1")
            data = res.json()[0]
            continent = data["region"]
            continent_cache[country_name] = continent
            return continent
        else:
            print("call_api_2")
            corrected_name = fallback_mapping.get(country_name)
            if corrected_name:
                if corrected_name in continent_cache:
                    continent = continent_cache[corrected_name]
                    continent_cache[country_name] = continent
                    return continent

                res = requests.get(f"https://restcountries.com/v3.1/name/{corrected_name}", timeout=5)
                if res.status_code == 200:
                    data = res.json()[0]
                    continent = data["region"]
                    continent_cache[country_name] = continent
                    continent_cache[corrected_name] = continent
                    return continent
            continent_cache[country_name] = f"Not found: {country_name}"
            return continent_cache[country_name]
    except Exception as e:
        continent_cache[country_name] = f"Error: {str(e)}"
        return continent_cache[country_name]
