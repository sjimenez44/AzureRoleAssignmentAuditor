import re
import os
import pandas as pd
import networkx as nx
from pyvis.network import Network
from fastapi import FastAPI
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse


def extract_target_info(target):
    # Caso de Management Group root
    if target == "/":
        return "Root", "ManagementGroup"
    mg_match = re.match(r"/providers/Microsoft\.Management/managementGroups/([^/]+)", target)
    if mg_match:
        return mg_match.group(1), "ManagementGroup"
    sub_match = re.match(r"/subscriptions/([^/]+)$", target)
    if sub_match:
        return sub_match.group(1), "Subscription"
    rg_match = re.match(r"/subscriptions/[^/]+/resourceGroups/([^/]+)$", target)
    if rg_match:
        return rg_match.group(1), "ResourceGroup"
    resource_match = re.match(r"/subscriptions/[^/]+/resourceGroups/[^/]+/providers/.*/([^/]+)$", target)
    if resource_match:
        return resource_match.group(1), "Resource"
    return "Unknown", "Unknown"


app = FastAPI()

if not os.path.exists("lib"):
    os.makedirs("lib")
app.mount("/lib", StaticFiles(directory="lib"), name="lib")


csv_files = [
    'data/subscriptions.csv', 'data/groups.csv', 'data/users.csv', 'data/app_role_assignments.csv', 
    'data/service_principals_roles.csv', 'data/resources.csv', 'data/role_assignments.csv', 
    'data/role_definitions.csv', 'data/nodes_types.csv'
]

missing_files = [file for file in csv_files if not os.path.exists(file)]
if missing_files:
    raise HTTPException(status_code=400, detail={"error": "Missing CSV files", "files": missing_files})


nodes_types = pd.read_csv('data/nodes_types.csv')
groups = pd.read_csv('data/groups.csv')
users = pd.read_csv('data/users.csv')
servicePrincipals = pd.read_csv('data/service_principals_roles.csv')
appRoleAssignments = pd.read_csv('data/app_role_assignments.csv')
subscriptions = pd.read_csv('data/subscriptions.csv')
roleDefinitions = pd.read_csv('data/role_definitions.csv')
roleAssignments = pd.read_csv('data/role_assignments.csv')
resources = pd.read_csv('data/resources.csv').assign(ResourceCustom="Resource")


EnrichedAppRoleAssignments = appRoleAssignments.merge(servicePrincipals[["AppRoleId", "AppRoleName", "AppType"]], on=["AppRoleId", "AppRoleId"], how="left")
EnrichedAzRoleAssignments = roleAssignments.merge(roleDefinitions[["RoleDefinitionId", "RoleName", "RoleType"]], on=["RoleDefinitionId", "RoleDefinitionId"], how="left")

PermissionsAppRoles = EnrichedAppRoleAssignments[["PrincipalName", "PrincipalType", "AppRoleName", "ResourceName", "AppType"]]\
    .merge(servicePrincipals[["AppDisplayName", "AppType"]].rename(columns={"AppDisplayName": "PrincipalName", "AppType": "AppCustom"}), on=["PrincipalName", "PrincipalName"],  how="left")\
    .assign(PrincipalType=lambda df: df["AppCustom"].fillna(df["PrincipalType"]))\
    .merge(resources[["ResourceName", "ResourceCustom"]].rename(columns={"ResourceName": "PrincipalName"}), on=["PrincipalName", "PrincipalName"], how="left")\
    .assign(PrincipalType=lambda df: df["ResourceCustom"].fillna(df["PrincipalType"]))[["PrincipalName", "PrincipalType", "AppRoleName", "ResourceName", "AppType"]]\
    .rename(columns={"PrincipalName": "Source", "PrincipalType": "SourceType", "AppRoleName": "Role", "ResourceName": "Target", "AppType": "TargetType"})

PermissionsAzRoles = EnrichedAzRoleAssignments[["PrincipalId", "PrincipalType", "RoleName", "Scope"]]\
    .merge(users[["UserId", "DisplayName"]].rename(columns={"UserId": "PrincipalId"}), on=["PrincipalId", "PrincipalId"], how="left")\
    .assign(PrincipalId=lambda df: df["DisplayName"].fillna(df["PrincipalId"]))[['PrincipalId', 'PrincipalType', 'RoleName', 'Scope']]\
    .merge(groups[["GroupId", "DisplayName"]].rename(columns={"GroupId": "PrincipalId"}), on=["PrincipalId", "PrincipalId"], how="left")\
    .assign(PrincipalId=lambda df: df["DisplayName"].fillna(df["PrincipalId"]))[['PrincipalId', 'PrincipalType', 'RoleName', 'Scope']]\
    .merge(resources[["PrincipalId", "ResourceName", "ResourceCustom"]], on=["PrincipalId", "PrincipalId"], how="left")\
    .assign(PrincipalId=lambda df: df["ResourceName"].fillna(df["PrincipalId"]))\
    .assign(PrincipalType=lambda df: df["ResourceCustom"].fillna(df["PrincipalType"]))[['PrincipalId', 'PrincipalType', 'RoleName', 'Scope']]\
    .merge(servicePrincipals[["ObjectId", "AppDisplayName", "AppType"]].rename(columns={"ObjectId": "PrincipalId", "AppType": "AppCustom"}), on=["PrincipalId", "PrincipalId"], how="left")\
    .assign(AppCustom=lambda df: df["AppCustom"].replace("ManagedIdentity", "Application"))\
    .assign(PrincipalId=lambda df: df["AppDisplayName"].fillna(df["PrincipalId"]))\
    .assign(PrincipalType=lambda df: df["AppCustom"].fillna(df["PrincipalType"]))[['PrincipalId', 'PrincipalType', 'RoleName', 'Scope']]\
    .rename(columns={"PrincipalId": "Source", "PrincipalType": "SourceType", "RoleName": "Role", "Scope": "Target"})
PermissionsAzRoles[["Target", "TargetType"]] = PermissionsAzRoles["Target"].apply(lambda x: pd.Series(extract_target_info(x)))
PermissionsAzRoles = PermissionsAzRoles.merge(subscriptions[["SubscriptionId", "DisplayName"]]\
                        .rename(columns={"SubscriptionId": "Target"}), on=["Target", "Target"], how="left")\
                        .assign(Target=lambda df: df["DisplayName"].fillna(df["Target"]))[['Source', 'SourceType', 'Role', 'Target', 'TargetType']]

connections = pd.concat([PermissionsAppRoles, PermissionsAzRoles]).reset_index(drop=True)\
    .merge(nodes_types.rename(columns={"Type": "SourceType", "Color": "SourceColor"}), on=["SourceType", "SourceType"], how="left")\
    .merge(nodes_types.rename(columns={"Type": "TargetType", "Color": "TargetColor"}), on=["TargetType", "TargetType"], how="left")\
    .drop_duplicates()\
        [["Source", "SourceType", "SourceColor", "Role", "Target", "TargetType", "TargetColor"]]


@app.get("/", response_class=HTMLResponse)
async def show_graph():
    G = nx.MultiDiGraph()
    for index, row in connections.iterrows():
        G.add_node(row["Source"], color=row["SourceColor"], type=row["SourceType"], title=f"Tipo: {row['SourceType']}", size=30)
        G.add_node(row["Target"], color=row["TargetColor"], type=row["TargetType"], title=f"Tipo: {row['TargetType']}", size=30)
        G.add_edge(row["Source"], row["Target"], key=index, label=row["Role"], smooth="curvedCW", arrows="to")

    net = Network(notebook=False, select_menu=True, filter_menu=True, directed=True)
    net.from_nx(G)
    net.repulsion(node_distance=240, central_gravity=0.15, spring_length=200, damping=0.6)
    net.options.interaction = {
        "dragNodes": True,
        "dragView": True,
        "zoomView": True
    }

    html_content = net.generate_html()

    return HTMLResponse(content=html_content)
