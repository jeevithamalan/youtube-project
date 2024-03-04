# Youtube Data Harvesting and Warehousing

The Main aim of this project is to allow users to access and analyze data from multiple YouTube channels.Based on the Analyzed data they can interpret various realtime scenarios

## Installation and Setup
Clone the repository:

```console
git clone https://github.com/jeevithamalan/youtube-project.git
```
Navigate to the project directory:

```console
cd youtube-project
```
Install dependencies using pip:

```console
pip install -r requirements.txt
```
Obtain API credentials from the Google Cloud Console and add them wherever necessary.

Run the Project:

```console
streamlit run youproject.py
```

You can view a live demo over at http://localhost:8501/

## Project Overview

The YouTube Data Harvesting and Warehousing project consists of the following components:

#### Streamlit Application:
A user-friendly UI built using Streamlit library, allowing users to interact with the application and perform data retrieval and analysis tasks.
#### YouTube API Integration: 
Integration with the YouTube API to fetch channel and video data based on the provided channel ID.
#### MongoDB Data:
Storage of the retrieved data in a MongoDB database, providing a flexible and scalable solution for storing unstructured and semi-structured data.
#### SQL Data Warehouse:
Migration of data from the data lake to a SQL database, allowing for efficient querying and analysis using SQL queries.
#### Data Visualization:
Presentation of retrieved data using Streamlit's data visualization features, enabling users to analyze the data through charts and graphs.

## Usage

Once the project is setup and running, users can access the Streamlit application through a web browser. The application will provide a user interface where users can perform the following actions:

* Enter a YouTube channel ID to retrieve data for that channel.
* Store the retrieved data in the MongoDB data lake.
* Collect and store data for multiple YouTube channels in the data lake.
* Select a channel and migrate its data from the data lake to the SQL data warehouse.
* Search and retrieve data from the SQL database using various search options.

## Contributing
Contributions to this project are welcome! If you encounter any issues or have suggestions for improvements, please feel free to submit a pull request.

## Conclusion

The YouTube Data Harvesting and Warehousing project provides a powerful tool for retrieving, storing, and analyzing YouTube channel and video data. By leveraging SQL, MongoDB, and Streamlit, users can easily access and manipulate YouTube data in a user-friendly interface. The project offers flexibility, scalability, and data querying capabilities, empowering users to gain insights from the vast amount of YouTube data available.

## References

* Streamlit Documentation: https://docs.streamlit.io/
* YouTube API Documentation: https://developers.google.com/youtube
* MongoDB Documentation: https://docs.mongodb.com/
* SQLAlchemy Documentation: https://docs.sqlalchemy.org/
* Python Documentation: https://docs.python.org/


