from sqlalchemy import create_engine, text

def get_engine(
    user: str = "oscaralateras",
    password: str = "",
    host: str = "localhost",
    port: int = 5432,
    database: str = "enigma_inventory"
):
    """
    Create a SQLAlchemy engine for connecting to the Postgres database.
    
    Returns a reusable engine object that manages database connections.
    This engine will be used for all database operations (create tables, insert, query).
    
    Args:
        user: Postgres username (defaults to your Mac user)
        password: Postgres password (empty for local dev with trust authentication)
        host: Database server hostname
        port: Database server port
        database: Name of the database to connect to
        
    Returns:
        SQLAlchemy Engine instance
    """
    # Build the connection string in the format: postgresql://user:pass@host:port/dbname
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"

    # Create and return the engine - this doesn't actually connect yet, just prepares the connection
    return create_engine(connection_string)

if __name__ == "__main__":
    # Test the connection
    engine = get_engine()

    # Try to connect and run a simple query
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("Connection successful")
   
