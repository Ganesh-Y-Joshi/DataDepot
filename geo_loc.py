base32 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
decode_map = {base32[i]: i for i in range(len(base32))}


def encode(latitude, longitude, precs: int = 5):
    """
    This encodes the provided latitude and longitude to
    geo-hash implementation.

    :param  precs:
    :param  latitude:
    :param  longitude:
    :return str:
    """
    bits = [16, 8, 4, 2, 1]
    cd = 0
    bit = 0
    geohash = []
    even = True
    lon_int, lat_int = (-180, 180), (-90, 90)
    while len(geohash) < precs:
        if even:
            mid = (lon_int[0] + lon_int[1]) / 2
            if longitude > mid:
                cd |= bits[bit]
                lon_int = (mid, lon_int[1])
            else:
                lon_int = (lon_int[0], mid)
        else:
            mid = (lat_int[0] + lat_int[1]) / 2
            if latitude > mid:
                cd |= bits[bit]
                lat_int = (mid, lat_int[1])
            else:
                lat_int = (lat_int[0], mid)
        even = not even
        if bit < 4:
            bit += 1
        else:
            geohash.append(base32[cd])
            cd = 0
            bit = 0
    return "".join(geohash)


def decode(geohash):
    """
    It decodes the provided geohash to provide latitude and longitude
    in the order of (lat, lon)

    :param  geohash:
    :return lat, lon:
    """
    is_even = True
    lon_int, lat_int = (-180, 180), (-90, 90)
    for c in geohash:
        cd = decode_map[c]
        for mask in [16, 8, 4, 2, 1]:
            if is_even:
                mid = (lon_int[0] + lon_int[1]) / 2
                if cd & mask:
                    lon_int = (mid, lon_int[1])
                else:
                    lon_int = (lon_int[0], mid)
            else:
                mid = (lat_int[0] + lat_int[1]) / 2
                if cd & mask:
                    lat_int = (mid, lat_int[1])
                else:
                    lat_int = (lat_int[0], mid)
            is_even = not is_even
    return (lat_int[0] + lat_int[1]) / 2, (lon_int[0] + lon_int[1]) / 2


def generate_grid(_points: dict, precs: int = 2):
    """
    It generates a grid containing all the points within
    a particular precision.

    precision = 2 means 5 - 10km on earth surface

    :param  _points:
    :param  precs:
    :return dict:
    """
    _grid = {}
    for key, val in _points.items():
        # Quantize latitude and longitude to 1km grid cells
        grid_lat = round(val[0], precs)  # round to 3 decimal places (approximately 1km resolution)
        grid_lon = round(val[1], precs)

        # Create a geohash based on the quantized grid cell
        geohash = encode(grid_lat, grid_lon)
        # Add the point to the grid cell
        if geohash not in _grid:
          
            _grid[geohash] = []
        _grid[geohash].append({key: (val[0], val[1])})

    return _grid
