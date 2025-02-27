def generate_plant_request(plant_id: str) -> dict:
    return {"PlantId": plant_id}

def generate_fuel_request(fuel: str) -> dict:
    return {"FuelType": fuel}

def generate_country_fuel_request(country_id: str, fuel_id: str) -> dict:
    return {"Country": country_id, "Fuel": fuel_id}


def process_country_fuel_response(response: dict) -> dict:
    return response["data"]
