from abc import ABC, abstractmethod
from azure.identity import DefaultAzureCredential, AzureCliCredential
import pandas as pd
import requests


class AzureAuthenticator:
    def __init__(self, use_managed_identity: bool = False):
        self.credential = DefaultAzureCredential() if use_managed_identity else AzureCliCredential()
    
    def get_access_token(self, resource: str) -> str:
        return self.credential.get_token(resource).token

class AzureAPIClient(ABC):
    BASE_URL = ""
    
    def __init__(self, authenticator: AzureAuthenticator, resource: str):
        self.token = authenticator.get_access_token(resource)
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json"
        }

    @abstractmethod
    def fetch_data(self, endpoint: str):
        pass

class AzureManagementClient(AzureAPIClient):
    BASE_URL = "https://management.azure.com"

    def __init__(self, authenticator: AzureAuthenticator):
        super().__init__(authenticator, "https://management.azure.com/.default")

    def fetch_data(self, url: str):
        response = requests.get(url, headers=self.headers)
        return response.json() if response.status_code == 200 else {}

    def get_subscriptions(self):
        url = f"{self.BASE_URL}/subscriptions?api-version=2020-01-01"
        return self.fetch_data(url).get("value", [])
    
    def get_role_assignments(self, subscription_id):
        url = f"{self.BASE_URL}/subscriptions/{subscription_id}/providers/Microsoft.Authorization/roleAssignments?api-version=2022-04-01"
        return self.fetch_data(url).get("value", [])

    def get_role_definitions(self, subscription_id):
        url = f"{self.BASE_URL}/subscriptions/{subscription_id}/providers/Microsoft.Authorization/roleDefinitions?api-version=2022-04-01"
        return self.fetch_data(url).get("value", [])
    
    def get_resources(self, subscription_id):
        url = f"{self.BASE_URL}/subscriptions/{subscription_id}/resources?api-version=2021-04-01"
        return self.fetch_data(url).get("value", [])

class MicrosoftGraphClient(AzureAPIClient):
    BASE_URL = "https://graph.microsoft.com/v1.0"

    def __init__(self, authenticator: AzureAuthenticator):
        super().__init__(authenticator, "https://graph.microsoft.com/.default")
    
    def fetch_data(self, url: str):
        response = requests.get(url, headers=self.headers)
        return response.json() if response.status_code == 200 else {}

    def get_service_principals(self):
        url = f"{self.BASE_URL}/servicePrincipals"
        return self.fetch_data(url).get("value", [])
    
    def get_app_role_assignments(self, app_id):
        url = f"{self.BASE_URL}/servicePrincipals(appId='{app_id}')/appRoleAssignedTo"
        return self.fetch_data(url).get("value", [])
    
    def get_users(self):
        url = f"{self.BASE_URL}/users"
        return self.fetch_data(url).get("value", [])
    
    def get_groups(self):
        url = f"{self.BASE_URL}/groups"
        return self.fetch_data(url).get("value", [])


class AzureDataProcessor:
    @staticmethod
    def process_subscription(client):
        data = []
        for subscription in client.get_subscriptions():
            data.append({
                "TenantId": subscription["tenantId"],
                "SubscriptionId": subscription["subscriptionId"],
                "DisplayName": subscription["displayName"],
                "State": subscription["state"]
            })
        df = pd.DataFrame(data)
        return df, df["SubscriptionId"].to_list()
    
    @staticmethod
    def process_role_assignments(subscriptions, client):
        data = []
        for sub in subscriptions:
            for assignment in client.get_role_assignments(sub):
                data.append({
                    "SubscriptionId": sub,
                    "RoleAssignmentId": assignment["id"],
                    "RoleAssignmentType": assignment["type"],
                    "RoleAssignmentName": assignment["name"],
                    "RoleDefinitionId": assignment["properties"]["roleDefinitionId"],
                    "PrincipalId": assignment["properties"]["principalId"],
                    "PrincipalType": assignment["properties"]["principalType"],
                    "Scope": assignment["properties"]["scope"],
                    "Condition": assignment["properties"]["condition"],
                    "ConditionVersion": assignment["properties"]["conditionVersion"],
                    "CreatedOn": assignment["properties"]["createdOn"],
                    "UpdatedOn": assignment["properties"]["updatedOn"],
                    "CreatedBy": assignment["properties"]["createdBy"],
                    "UpdatedBy": assignment["properties"]["updatedBy"],
                    "DelegatedMIResourceId": assignment["properties"]["delegatedManagedIdentityResourceId"],
                    "Description": assignment["properties"]["description"]
                })
        return pd.DataFrame(data)

    @staticmethod
    def process_role_definitions(subscriptions, client):
        data = []
        for sub in subscriptions:
            for role in client.get_role_definitions(sub):
                data.append({
                    "SubscriptionId": sub,
                    "RoleDefinitionId": role["id"],
                    "RoleDefinitionType": role["type"],
                    "RoleDefinitionName": role["name"],
                    "RoleName": role["properties"]["roleName"],
                    "RoleType": role["properties"]["type"],
                    "Description": role["properties"]["description"],
                    "AssignableScopes": role["properties"]["assignableScopes"],
                    "CreatedOn": role["properties"]["createdOn"],
                    "UpdatedOn": role["properties"]["updatedOn"],
                    "CreatedBy": role["properties"]["createdBy"],
                    "UpdatedBy": role["properties"]["updatedBy"],
                })
        return pd.DataFrame(data)
    
    @staticmethod
    def process_resources(subscriptions, client):
        data = []
        for sub in subscriptions:
            for resource in client.get_resources(sub):
                data.append({
                    "ResourceId": resource["id"],
                    "ResourceName": resource["name"],
                    "ResourceType": resource["type"],
                    "ResourceLocation": resource["location"],
                    "PrincipalId": resource.get("identity", {}).get("principalId", None),
                    "PrincipalType": resource.get("identity", {}).get("type", None),
                })
        return pd.DataFrame(data)
    
    @staticmethod
    def process_service_principals(client):
        data = []
        app_ids = set()
        for service_principal in client.get_service_principals():
            if (service_principal["appRoles"]):
                for app_role in service_principal["appRoles"]:
                    data.append({
                        "ObjectId": service_principal["id"],
                        "AppId": service_principal["appId"],
                        "AppDisplayName": service_principal["displayName"],
                        "AppCreationDate": service_principal["createdDateTime"],
                        "AppType": service_principal["servicePrincipalType"],
                        "AppRoleDescription": app_role["description"],
                        "AppRoleDisplayName": app_role["displayName"],
                        "AppRoleId": app_role["id"],
                        "AppRoleOrigin": app_role["origin"],
                        "AppRoleName": app_role["value"],
                    })
                    app_ids.add(service_principal["appId"])
            else:
                data.append({
                    "ObjectId": service_principal["id"],
                    "AppId": service_principal["appId"],
                    "AppDisplayName": service_principal["displayName"],
                    "AppCreationDate": service_principal["createdDateTime"],
                    "AppType": service_principal["servicePrincipalType"],
                    "AppRoleDescription": None,
                    "AppRoleDisplayName": None,
                    "AppRoleId": None,
                    "AppRoleOrigin": None,
                    "AppRoleName": None,
                })
        return pd.DataFrame(data), app_ids
    
    @staticmethod
    def process_app_role_assignments(client, app_ids):
        data = []
        for app_id in app_ids:
            for assignment in client.get_app_role_assignments(app_id):
                data.append({
                    "AppRoleId": assignment["appRoleId"],
                    "CreatedDateTime": assignment["createdDateTime"],
                    "PrincipalName": assignment["principalDisplayName"],
                    "PrincipalId": assignment["principalId"],
                    "PrincipalType": assignment["principalType"],
                    "ResourceName": assignment["resourceDisplayName"],
                    "ResourceId": assignment["resourceId"],
                })
        return pd.DataFrame(data)
    
    @staticmethod
    def process_users(client):
        data = []
        for user in client.get_users():
            data.append({
                "UserId": user["id"],
                "UserPrincipalName": user["userPrincipalName"],
                "DisplayName": user["displayName"],
                "Type": "User",
            })
        return pd.DataFrame(data)
    
    @staticmethod
    def process_groups(client):
        data = []
        for group in client.get_groups():
            data.append({
                "GroupId": group["id"],
                "DisplayName": group["displayName"],
                "CreatedDate": group["createdDateTime"],
                "Type": "Group",
            })
        return pd.DataFrame(data)


if __name__ == "__main__":
    authenticator = AzureAuthenticator()
    # Token: az account get-access-token --resource https://management.azure.com/
    management_client = AzureManagementClient(authenticator)
    # Token: az account get-access-token --resource https://graph.microsoft.com/
    graph_client = MicrosoftGraphClient(authenticator)

    print("Saving subscriptions...")
    df_subscription, subscriptions = AzureDataProcessor.process_subscription(management_client)
    df_subscription.to_csv("data/subscriptions.csv", index=False)
    print("Saving resources...")
    df_resources = AzureDataProcessor.process_resources(subscriptions, management_client)
    df_resources.to_csv("data/resources.csv", index=False)
    print("Saving role assignments...")
    df_role_assignments = AzureDataProcessor.process_role_assignments(subscriptions, management_client)
    df_role_assignments.to_csv("data/role_assignments.csv", index=False)
    print("Saving role definitions...")
    df_role_definitions = AzureDataProcessor.process_role_definitions(subscriptions, management_client)
    df_role_definitions.to_csv("data/role_definitions.csv", index=False)
    print("Saving service principals...")
    df_service_principals, app_ids = AzureDataProcessor.process_service_principals(graph_client)
    df_service_principals.to_csv("data/service_principals_roles.csv", index=False)
    print("Saving application role assignments...")
    df_app_role_assignments = AzureDataProcessor.process_app_role_assignments(graph_client, app_ids)
    df_app_role_assignments.to_csv("data/app_role_assignments.csv", index=False)
    print("Saving users...")
    df_users = AzureDataProcessor.process_users(graph_client)
    df_users.to_csv("data/users.csv", index=False)
    print("Saving groups...")
    df_groups = AzureDataProcessor.process_groups(graph_client)
    df_groups.to_csv("data/groups.csv", index=False)
    print("Saving node types...")
    df_types = pd.DataFrame([
        {"Type": "User",             "Color": "cyan"},
        {"Type": "Group",            "Color": "blue"},
        {"Type": "ServicePrincipal", "Color": "gray"},
        {"Type": "Application",      "Color": "red"},
        {"Type": "ManagementGroup",  "Color": "brown"},
        {"Type": "Subscription",     "Color": "purple"},
        {"Type": "ResourceGroup",    "Color": "yellow"},
        {"Type": "Resource",         "Color": "orange"},
    ])
    df_types.to_csv("data/nodes_types.csv", index=False)