## Microsoft Purview brief introduction
[Microsoft Purview](https://learn.microsoft.com/en-us/purview/purview) is a family of data governance, risk, and compliance solutions that can help your organization govern, protect, and manage your entire data estate.

Microsoft Purview combines the former [Azure Purview](https://learn.microsoft.com/en-us/azure/purview/) and [Microsoft 365 compliance](https://learn.microsoft.com/en-us/microsoft-365/compliance/?view=o365-worldwide) solutions and services together into a single brand. Together, these solutions help your organization to:

- Gain visibility into data assets across your organization
- Enable access to your data, security, and risk solutions
- Safeguard and manage sensitive data across clouds, apps, and endpoints
- Manage end-to-end data risks and regulatory compliance
- Empower your organization to govern, protect, and manage data in new, comprehensive ways

While in this sample application, we'll focus on the integration with Purview Data Map, which is the foundation of the Purview data governance.
The use case would looks like below.
1. Source data flows through the config driven data pipelines and reaches to serving zone finally.
2. Processed data of different zones, staing, standard and serving, is stored in ADLS Gen2.
3. Register relevant ADLS Gen2 as data source in Azure Purview, run scan and save the scanned metadata as data assets.
4. Create a lineage with relevant data assets to describe the whole data processing lifecycle.

## Purview integration prerequisites

1. Create a Azure Purview account.
2. Create a service principal (or use an existing one) and grant it with below Purview roles in Purview Governance Portal, by going to Data Map -> Collections -> Root Purview account -> Role assignments view.
   - Collection admins
   - Data source admins
   - Data curators
3. Set client id and secret as environment variables in the same environment where the sample application is deployed and running.
4. Set Purview account name and storage account (ADLS Gen2) info in sample application configuration UI.
