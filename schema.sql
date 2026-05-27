CREATE TABLE IF NOT EXISTS documents (
    document_id UUID PRIMARY KEY,
    document_type VARCHAR(50),
    file_name TEXT,
    file_path TEXT,
    source VARCHAR(100),
    status VARCHAR(50),
    processed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS invoice_data (
    invoice_id UUID PRIMARY KEY,
    document_id UUID REFERENCES documents(document_id),
    invoice_number VARCHAR(100),
    vendor_name TEXT,
    invoice_date VARCHAR(50),
    total_amount NUMERIC
);

CREATE TABLE IF NOT EXISTS bank_transactions (
    transaction_id UUID PRIMARY KEY,
    document_id UUID REFERENCES documents(document_id),
    transaction_date DATE,
    description TEXT,
    amount NUMERIC,
    balance NUMERIC
);

CREATE TABLE IF NOT EXISTS processing_logs (
    log_id UUID PRIMARY KEY,
    document_id UUID,
    stage VARCHAR(100),
    status VARCHAR(50),
    message TEXT,
    created_at TIMESTAMP
);