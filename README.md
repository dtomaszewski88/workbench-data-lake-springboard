# MayStreet Workbench Neo Data Lake Springboard

## Welcome

Welcome to the Data Lake Springboard

This Springboard provides a full set of example queries that can run inside Workbench Neo and retrieve data
from our Data Lake platform.

We provide multiple JupyterLab Notebooks full of example queries you may open and run inside Workbench Neo; please
consult the rest of this document for a brief introduction to each Notebook.

Feel free to copy the code from our Notebooks into your own Notebook, or play around with the existing Notebook. 
This is a completely isolated copy which will stay in your own secured Workbench file system, so you're more than 
welcome to tinker with it as much as you'd like!

If you'd like to make a copy of the Notebook with a different name, select `File` -> `Save As...` to save the Notebook
with a new name.

## Notebooks

### _data-lake-introduction.ipynb_

This Notebook will provide a very simple query that can be run to retrieve data from our Data Lake and show it
inside the Notebook output.

To run this Notebook, simply open it, and click the "Run All" button in the top toolbar.

### _visualising-data-lake-output.ipynb_

This Notebook will retrieve AAPL data grouped by feed for the 07th January 2022, and display the output on a very
simple 2D graph.

To run this Notebook, simply open it, and click the "Run All" button in the top toolbar.

### _distributed-data-lake-queries-to-graph.ipynb_

This complicated Notebook will invoke Data Lake queries throughout a distributed Dask cluster, and will then 
collect the response and create an example 3D graph.

You will need to create a Dask cluster before you can run this sample; to do so, please follow the short video
here:
![images/provisioning-cluster.gif](/images/provisioning-cluster.gif)

Once the Dask cluster has moved to the provisioning state, update the address of the cluster inside the Notebook; 
change the following line:

`cluster_client = Client("tcp://10.0.0.44:8786")`

to 

`cluster_client = Client("tcp://<<ip address and port shown inside the Clusters sidebar>>")`

Once you have updated the client to point to the correct Dask cluster, click the "Run All" button in the top toolbar
to run each of the cells in turn.

## Support

For support, please email <support@maystreet.com>. Thank you.
