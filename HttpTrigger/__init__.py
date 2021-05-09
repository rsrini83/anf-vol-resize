import logging
import json
import traceback
import azure.functions as func
import os
from azure.identity import ClientSecretCredential
from azure.identity import ManagedIdentityCredential
from azure.mgmt.netapp import NetAppManagementClient

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
        This azure function needs to be configured for ANF volume usage alert,
        volume's usage threshold will be increased based on the resize percentage value.
        if the resize percentage value is not configured then it uses default value(5%).
        Configure below environment variables,
        RESIZE_PERCENTAGE   => Should be an integer. Ex: 5
        IS_RESIZE_ENABLED   => Indicates whether resize functionality enabled or not. Ex: true. Default is false
        ## Create new app registration and provide proper permissions for volumes
        CLIENT_ID           => App registration client id
        CLIENT_SECRET       => App registration client secret
        TENANT_ID           => Tenant Id
    """
    logging.info('[ResizeVol]================== Start =======================')
    req_body = req.get_json()
    resource_ids = req_body["data"]["essentials"]["alertTargetIDs"]
    if "RESIZE_PERCENTAGE" in os.environ:
        increase_pct = os.environ["RESIZE_PERCENTAGE"]
    else:
        increase_pct = 0.05
    if "IS_RESIZE_ENABLED" in os.environ:
        IS_RESIZE_ENBALED = os.environ["IS_RESIZE_ENABLED"]
    else:
        IS_RESIZE_ENBALED = "false"
    
    if  "CLIENT_ID" not in os.environ or \
        "CLIENT_SECRET" not  in os.environ or \
        "TENANT_ID" not in os.environ:
        logging.error("[ResizeVol] Missing mandatory parameters - CLIENT_ID, CLIENT_SECRET, TENANT_ID")
        return func.HttpResponse(
             "Resize volume function executed with error - ERR-001",
             status_code=400
        )        

    try:
        increase_pct = int(increase_pct) / 100
    except:
        logging.warning("[ResizeVol]Unable parse the resize percentage")
        increase_pct = 0.05
    subscription_id= get_subscription(resource_ids[0])
    resource_group =get_resource_group(resource_ids[0])
    capacity_pool = get_anf_capacity_pool(resource_ids[0])
    account_name = get_anf_account(resource_ids[0])
    credentials = ClientSecretCredential(
        client_id= os.environ["CLIENT_ID"],# '6507e879-e79e-4fdc-9c81-85b2e4352e75',
        client_secret=os.environ["CLIENT_SECRET"],#'_DHiZvx58z~~Qv.nyb_rVlt72pstKB~8-l',
        tenant_id=os.environ["TENANT_ID"],#'bf8aa3a3-ba5e-47f1-8e94-d736c547c1c6'
    )
    anf_client = NetAppManagementClient(credentials, subscription_id)
    logging.info("[ResizeVol] Retreiving the volumes")
    logging.info("[ResizeVol] ANF volume name is:" + get_anf_volume(resource_ids[0]))
    logging.info("[ResizeVol] Retrieving volume details")
    anf_volume = anf_client.volumes.get(resource_group, account_name, capacity_pool, get_anf_volume(resource_ids[0]))
    usage_threshold = anf_volume.usage_threshold
    usage_threshold = usage_threshold * increase_pct + usage_threshold
    if IS_RESIZE_ENBALED == "true":
        logging.info("[ResizeVol] New threshold would be:" + str(usage_threshold))
        update_res = anf_client.volumes.begin_update(resource_group, account_name, capacity_pool, get_anf_volume(resource_ids[0])
        ,{"usageThreshold": usage_threshold})
        logging.info(update_res.result())
    else:
        logging.warning("[ResizeVol] Resize volume is disabled, please enable to resize it to - " + str(usage_threshold))
    logging.info('[ResizeVol]================== Start =======================')
    return func.HttpResponse(
             "Resize volume function executed successfully",
             status_code=200
        )



def get_resource_value(resource_uri, resource_name):
    """Gets the resource name based on resource type
    Function that returns the name of a resource from resource id/uri based on
    resource type name.
    Args:
        resource_uri (string): resource id/uri
        resource_name (string): Name of the resource type, e.g. capacityPools
    Returns:
        string: Returns the resource name
    """

    if not resource_uri.strip():
        return None

    if not resource_name.startswith('/'):
        resource_name = '/{}'.format(resource_name)

    if not resource_uri.startswith('/'):
        resource_uri = '/{}'.format(resource_uri)

    # Checks to see if the ResourceName and ResourceGroup is the same name and
    # if so handles it specially.
    rg_resource_name = '/resourceGroups{}'.format(resource_name)
    rg_index = resource_uri.lower().find(rg_resource_name.lower())
    # dealing with case where resource name is the same as resource group
    if rg_index > -1:
        removed_same_rg_name = resource_uri.lower().split(
            resource_name.lower())[-1]
        return removed_same_rg_name.split('/')[1]

    index = resource_uri.lower().find(resource_name.lower())
    if index > -1:
        res = resource_uri[index + len(resource_name):].split('/')
        if len(res) > 1:
            return res[1]

    return None


def get_resource_name(resource_uri):
    """Gets the resource name from resource id/uri
    Function that returns the name of a resource from resource id/uri, this is
    independent of resource type
    Args:
        resource_uri (string): resource id/uri
    Returns:
        string: Returns the resource name
    """

    if not resource_uri.strip():
        return None

    position = resource_uri.rfind('/')
    return resource_uri[position + 1:]


def get_resource_group(resource_uri):
    """Gets the resource group name from resource id/uri
    Function that returns the resource group name from resource id/uri
    Args:
        resource_uri (string): resource id/uri
    Returns:
        string: Returns the resource group name
    """

    if not resource_uri.strip():
        return None

    return get_resource_value(resource_uri, '/resourceGroups')


def get_subscription(resource_uri):
    """Gets the subscription id from resource id/uri
    Function that returns the resource group name from resource id/uri
    Args:
        resource_uri (string): resource id/uri
    Returns:
        string: Returns the subcription id (GUID)
    """

    if not resource_uri.strip():
        return None

    return get_resource_value(resource_uri, '/subscriptions')


def get_anf_account(resource_uri):
    """Gets an account name from resource id/uri
    Function that returns the ANF acount name from resource id/uri
    Args:
        resource_uri (string): resource id/uri
    Returns:
        string: Returns the account name
    """

    if not resource_uri.strip():
        return None

    return get_resource_value(resource_uri, '/netAppAccounts')


def get_anf_capacity_pool(resource_uri):
    """Gets pool name from resource id/uri
    Function that returns the capacity pool name from resource id/uri
    Args:
        resource_uri (string): resource id/uri
    Returns:
        string: Returns the capacity pool name
    """

    if not resource_uri.strip():
        return None

    return get_resource_value(resource_uri, '/capacityPools')


def get_anf_volume(resource_uri):
    """Gets volume name from resource id/uri
    Function that returns the volume name from resource id/uri
    Args:
        resource_uri (string): resource id/uri
    Returns:
        string: Returns the volume name
    """

    if not resource_uri.strip():
        return None

    return get_resource_value(resource_uri, '/volumes')


def get_anf_snapshot(resource_uri):
    """Gets snapshot name from resource id/uri
    Function that returns the snapshot name from resource id/uri
    Args:
        resource_uri (string): resource id/uri
    Returns:
        string: Returns the snapshot name
    """

    if not resource_uri.strip():
        return None

    return get_resource_value(resource_uri, '/snapshots')


def is_anf_resource(resource_uri):
    """Checks if resource is an ANF related resource
    Function verifies if the resource referenced in the resource id/uri is an
    ANF related resource
    Args:
        resource_uri (string): resource id/uri
    Returns:
        boolean: Returns true if resource is related to ANF or false otherwise
    """

    if not resource_uri.strip():
        return False

    return resource_uri.find('/providers/microsoft.netApp/netAppAccounts') > -1


def is_anf_snapshot(resource_uri):
    """Checks if resource is a snapshot
    Function verifies if the resource referenced in the resource id/uri is a
    snapshot
    Args:
        resource_uri (string): resource id/uri
    Returns:
        boolean: Returns true if resource is a snapshot
    """

    if (not resource_uri.strip()) or (not is_anf_resource(resource_uri)):
        return False

    return resource_uri.rfind('/snapshots/') > -1


def is_anf_volume(resource_uri):
    """Checks if resource is a volume
    Function verifies if the resource referenced in the resource id/uri is a
    volume
    Args:
        resource_uri (string): resource id/uri
    Returns:
        boolean: Returns true if resource is a volume
    """
    
    if (not resource_uri.strip()) or (not is_anf_resource(resource_uri)):
        return False

    # return (resource_uri.rfind('/snapshots/') == -1) \
    #     and (resource_uri.rfind('/volumes/') > -1)

        
    return (not is_anf_snapshot(resource_uri)) \
        and (resource_uri.rfind('/volumes/') > -1)


def is_anf_capacity_pool(resource_uri):
    """Checks if resource is a capacity pool
    Function verifies if the resource referenced in the resource id/uri is a
    capacity pool
    Args:
        resource_uri (string): resource id/uri
    Returns:
        boolean: Returns true if resource is a capacity pool
    """

    if (not resource_uri.strip()) or (not is_anf_resource(resource_uri)):
        return False

    return (not is_anf_snapshot(resource_uri)) \
        and (not is_anf_volume(resource_uri)) \
        and (resource_uri.rfind('/capacityPools/') > -1)


def is_anf_account(resource_uri):
    """Checks if resource is an account
    Function verifies if the resource referenced in the resource id/uri is an
    account
    Args:
        resource_uri (string): resource id/uri
    Returns:
        boolean: Returns true if resource is an account
    """

    if (not resource_uri.strip()) or (not is_anf_resource(resource_uri)):
        return False

    return (not is_anf_snapshot(resource_uri)) \
        and (not is_anf_volume(resource_uri)) \
        and (not is_anf_capacity_pool(resource_uri)) \
        and (resource_uri.rfind('/backupPolicies/') == -1) \
        and (resource_uri.rfind('/netAppAccounts/') > -1)