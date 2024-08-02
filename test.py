from geopy.geocoders import Nominatim
geolocator = Nominatim(user_agent="Mozilla/5.0")
location = geolocator.reverse("52.509669, 13.376294")
print(location.address)
