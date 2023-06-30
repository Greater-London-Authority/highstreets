# User Guide for APIClient

Author: Anupam Bose
This user guide provides an overview of how to use the `APIClient` code to generate an access token for API authentication.

## Prerequisites

Before using the `APIClient` code, ensure you have the following:

- Python installed on your machine.
- The required dependencies installed. Please follow the guide mentioned in readme to install poetry.

## Usage


1. Set up the environment variables:
    - Create a `.env` file in the project directory.
    - Add the following lines to the `.env` file:
        ```dotenv
        CONSUMER_KEY=your_consumer_key
        CONSUMER_SECRET=your_consumer_secret
        ```

2. Use the `APIClient` in your code:
    - Import the necessary modules:
        ```python
        from apiclient import APIClient
        ```

    - Call the `get_access_token` method to obtain the access token:
        ```python
        access_token = APIClient.get_access_token()
        ```

    - Use the `access_token` for API authentication.

## Logging and Error Handling

The `APIClient` code includes basic logging and error handling. The log messages are written to the console. If an error occurs during the token generation process, an exception will be raised, and an error message will be logged.

To adjust the log level or configure log handlers, modify the logging configuration in the code according to your requirements.

## Troubleshooting

If you encounter any issues or errors while using the `APIClient` code, consider the following troubleshooting steps:

- Ensure the environment variables (`CONSUMER_KEY` and `CONSUMER_SECRET`) are correctly set in the `.env` file.
- Check your internet connection to ensure the code can make HTTP requests to the token endpoint.
- Review the error message and traceback to identify the cause of the issue.

If the problem persists, seek assistance from the repository maintainer or consult the project's issue tracker on GitHub.

## Further Customization

The provided code serves as a starting point and can be customized to fit your specific requirements. You may modify the code to handle additional API endpoints, implement different authentication methods, or integrate it into your existing codebase.

Refer to the official Python documentation and relevant libraries' documentation for more information on advanced usage and customization.

## Conclusion

The `APIClient` code provides a convenient way to obtain an access token for API authentication. By following the steps outlined in this user guide, you can easily integrate it into your projects and extend it to meet your specific needs.

If you have any questions or need further assistance, please reach out to Anupam Bose - anupam.bose@london.gov.uk.
