DROP TABLE IF EXISTS invoice_data;
DROP TABLE IF EXISTS processing_logs;
DROP TABLE IF EXISTS documents;

CREATE TABLE documents (
    document_id UUID PRIMARY KEY,
    document_type VARCHAR(50),
    file_name TEXT,
    file_path TEXT,
    source VARCHAR(50),
    status VARCHAR(30),
    received_at TIMESTAMP DEFAULT NOW(),
    processed_at TIMESTAMP
);

CREATE TABLE invoice_data (
    invoice_id UUID PRIMARY KEY,
    document_id UUID REFERENCES documents(document_id),

    invoice_number TEXT,
    vendor_name TEXT,
    gstin TEXT,
    invoice_date TEXT,
    total_amount NUMERIC,

    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE processing_logs (
    log_id SERIAL PRIMARY KEY,
    document_id UUID REFERENCES documents(document_id),
    log_level VARCHAR(20),
    message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);