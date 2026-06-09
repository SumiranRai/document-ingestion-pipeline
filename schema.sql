-- Document Package Management
CREATE TABLE IF NOT EXISTS document_packages (
    package_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    package_name VARCHAR(255) NOT NULL,
    source VARCHAR(100),
    total_documents INT DEFAULT 0,
    status VARCHAR(50) DEFAULT 'PENDING',
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Main Document Tracking
CREATE TABLE IF NOT EXISTS documents (
    document_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    package_id UUID REFERENCES document_packages(package_id),
    document_type VARCHAR(50) NOT NULL,
    file_name TEXT NOT NULL,
    file_path TEXT NOT NULL,
    source VARCHAR(100),
    file_size INT,
    checksum VARCHAR(64),
    status VARCHAR(50) DEFAULT 'RECEIVED',
    extraction_status VARCHAR(50) DEFAULT 'PENDING',
    validation_status VARCHAR(50) DEFAULT 'PENDING',
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    extraction_started_at TIMESTAMP,
    extraction_completed_at TIMESTAMP,
    processed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_status CHECK (status IN ('RECEIVED', 'PROCESSING', 'EXTRACTED', 'VALIDATED', 'PERSISTED', 'FAILED')),
    CONSTRAINT valid_extraction_status CHECK (extraction_status IN ('PENDING', 'IN_PROGRESS', 'COMPLETED', 'FAILED')),
    CONSTRAINT valid_validation_status CHECK (validation_status IN ('PENDING', 'IN_PROGRESS', 'PASSED', 'FAILED'))
);

-- Invoice Extraction and Data
CREATE TABLE IF NOT EXISTS invoices (
    invoice_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL UNIQUE REFERENCES documents(document_id) ON DELETE CASCADE,
    invoice_number VARCHAR(100),
    invoice_date DATE,
    vendor_name TEXT,
    vendor_gstin VARCHAR(15),
    customer_name TEXT,
    customer_gstin VARCHAR(15),
    subtotal NUMERIC(15,2),
    tax_amount NUMERIC(15,2),
    total_amount NUMERIC(15,2),
    currency VARCHAR(3) DEFAULT 'INR',
    payment_status VARCHAR(50),
    extraction_confidence NUMERIC(3,2),
    raw_extracted_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bank Statement Processing
CREATE TABLE IF NOT EXISTS bank_statements (
    statement_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL UNIQUE REFERENCES documents(document_id) ON DELETE CASCADE,
    bank_name VARCHAR(100),
    account_number VARCHAR(50),
    statement_period_from DATE,
    statement_period_to DATE,
    opening_balance NUMERIC(15,2),
    closing_balance NUMERIC(15,2),
    total_debits NUMERIC(15,2),
    total_credits NUMERIC(15,2),
    currency VARCHAR(3) DEFAULT 'INR',
    transaction_count INT DEFAULT 0,
    raw_extracted_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bank Transactions Detail
CREATE TABLE IF NOT EXISTS bank_transactions (
    transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    statement_id UUID NOT NULL REFERENCES bank_statements(statement_id) ON DELETE CASCADE,
    document_id UUID REFERENCES documents(document_id),
    transaction_date DATE,
    value_date DATE,
    description TEXT,
    transaction_type VARCHAR(10),
    amount NUMERIC(15,2),
    running_balance NUMERIC(15,2),
    reference_number VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ID Document Extraction and Data (PAN, Aadhaar, etc.)
CREATE TABLE IF NOT EXISTS id_documents (
    id_document_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL UNIQUE REFERENCES documents(document_id) ON DELETE CASCADE,
    id_type VARCHAR(50) NOT NULL,
    id_number VARCHAR(50),
    holder_name TEXT,
    dob DATE,
    gender VARCHAR(1),
    father_name TEXT,
    address TEXT,
    extraction_confidence NUMERIC(3,2),
    raw_extracted_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Processing Logs and Audit Trail
CREATE TABLE IF NOT EXISTS processing_logs (
    log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID REFERENCES documents(document_id) ON DELETE CASCADE,
    package_id UUID REFERENCES document_packages(package_id) ON DELETE CASCADE,
    stage VARCHAR(100),
    status VARCHAR(50),
    message TEXT,
    error_details TEXT,
    duration_ms INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Kafka Event Log for audit
CREATE TABLE IF NOT EXISTS kafka_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic_name VARCHAR(100),
    message_key VARCHAR(255),
    message_value JSONB,
    event_type VARCHAR(100),
    document_id UUID REFERENCES documents(document_id) ON DELETE SET NULL,
    partition INT,
    msg_offset BIGINT,
    published_at TIMESTAMP,
    consumed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Validation Rules Metadata
CREATE TABLE IF NOT EXISTS validation_rules (
    rule_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_type VARCHAR(50),
    field_name VARCHAR(100),
    rule_type VARCHAR(50),
    rule_definition JSONB,
    is_required BOOLEAN DEFAULT FALSE,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Extraction Statistics
CREATE TABLE IF NOT EXISTS extraction_stats (
    stat_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_type VARCHAR(50),
    total_documents INT DEFAULT 0,
    successful_extractions INT DEFAULT 0,
    failed_extractions INT DEFAULT 0,
    average_extraction_time_ms NUMERIC,
    date DATE DEFAULT CURRENT_DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(status);
CREATE INDEX IF NOT EXISTS idx_documents_type ON documents(document_type);
CREATE INDEX IF NOT EXISTS idx_documents_package ON documents(package_id);
CREATE INDEX IF NOT EXISTS idx_documents_created ON documents(created_at);
CREATE INDEX IF NOT EXISTS idx_packages_status ON document_packages(status);
CREATE INDEX IF NOT EXISTS idx_invoices_vendor ON invoices(vendor_name);
CREATE INDEX IF NOT EXISTS idx_bank_transactions_date ON bank_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_id_documents_type ON id_documents(id_type);
CREATE INDEX IF NOT EXISTS idx_id_documents_number ON id_documents(id_number);
CREATE INDEX IF NOT EXISTS idx_processing_logs_stage ON processing_logs(stage);
CREATE INDEX IF NOT EXISTS idx_kafka_events_topic ON kafka_events(topic_name);
CREATE INDEX IF NOT EXISTS idx_kafka_events_doc ON kafka_events(document_id);