"""Util functions for integration"""
import logging

from homeassistant.core import HomeAssistant

from .const import MetadataFilter, PropertyToEntityDescription, DOMAIN, NAME

_LOGGER: logging.Logger = logging.getLogger(__package__)


def get_ha_go2rtc_client(hass: HomeAssistant):
    """Return (session, base_url) for HA's own internal go2rtc instance, or
    (None, None) if it can't be found.

    HA (2023.4+) runs go2rtc embedded in Core, with an aiohttp ClientSession
    that's already wired up to talk to it correctly - over its Unix domain
    socket when no TCP API port is exposed, and with the right auth headers
    when go2rtc's local_auth is on (which HA always sets). Reusing that
    session, rather than guessing a TCP host:port ourselves, is the only way
    to reliably reach go2rtc's API on a modern HA install: the RTSP port is a
    fixed constant (see HA_MANAGED_GO2RTC_RTSP_PORT), but the API is
    Unix-socket-only by default and its credentials are generated at random
    per boot and never written to disk anywhere we could read them.

    This reaches into a private (underscore-prefixed) attribute of HA core's
    go2rtc integration because it exposes no public API for this - so it's
    intentionally defensive and returns (None, None) on any failure instead
    of raising, letting callers fall back to the legacy fixed-port behavior.
    """
    try:
        from homeassistant.components.go2rtc import (  # pylint: disable=import-outside-toplevel
            _DATA_GO2RTC,
        )

        go2rtc_config = hass.data.get(_DATA_GO2RTC)
        if go2rtc_config is None:
            return None, None
        return go2rtc_config.session, go2rtc_config.url
    except Exception as ex:  # pylint: disable=broad-except
        _LOGGER.debug(f"get_ha_go2rtc_client - could not reach HA's internal go2rtc client - {ex}")
        return None, None


def get_properties_by_filter(metadata: dict, filtering: MetadataFilter) -> dict:
    """Filter properties based on attributes for presentation"""
    result = {}
    for name, value in metadata.items():
        if (name in PropertyToEntityDescription.__members__) is False:
            continue
        if value.readable is not filtering.readable:
            continue
        if value.writeable is not filtering.writeable:
            continue
        if value.type in filtering.types:
            to_add = False

            if filtering.any_fields is None and filtering.no_fields is None:
                to_add = True
            else:
                if filtering.any_fields is not None:
                    for field in filtering.any_fields:
                        if value.__dict__.get(field, None) is not None:
                            to_add = True
                            break

                if filtering.no_fields is not None:
                    count_no_fields = len(filtering.no_fields)
                    for field in filtering.no_fields:
                        if value.__dict__.get(field, None) is not None:
                            count_no_fields = -1
                            break
                        else:
                            count_no_fields = count_no_fields - 1
                    if count_no_fields == 0:
                        to_add = True
            if to_add is True:
                result[name] = value
    return result


def get_product_properties_by_filter(lists: [], filtering: MetadataFilter):
    """Get product properties entitites list by filter"""
    product_properties = []
    for products in lists:
        for product in products:
            metadatas = get_properties_by_filter(product.metadata, filtering)
            for value in metadatas.values():
                product_properties.append(value)
    return product_properties


def get_device_info(product):
    """generate device info dict"""
    return {
        "identifiers": {(DOMAIN, product.serial_no)},
        "name": product.name,
        "model": product.model,
        "sw_version": product.software_version,
        "manufacturer": NAME,
    }
