// DOM elements
console.log('Loading Close to Pin DOM elements...');
const form = document.getElementById('close-to-pin-form');

// Load standards info on page load
loadStandardsInfo();
const submitBtn = document.getElementById('submit-btn');
const btnText = submitBtn?.querySelector('.btn-text');
const loadingSpinner = submitBtn?.querySelector('.loading-spinner');
const resultsSection = document.getElementById('results-section');
const resultsContent = document.getElementById('results-content');
const errorSection = document.getElementById('error-section');
const errorContent = document.getElementById('error-content');

// Check if all required elements are found
if (!form || !submitBtn || !btnText || !loadingSpinner || !resultsSection || !resultsContent || !errorSection || !errorContent) {
    console.error('Missing required DOM elements:', {
        form: !!form,
        submitBtn: !!submitBtn,
        btnText: !!btnText,
        loadingSpinner: !!loadingSpinner,
        resultsSection: !!resultsSection,
        resultsContent: !!resultsContent,
        errorSection: !!errorSection,
        errorContent: !!errorContent
    });
}

// Form submission handler
if (form) {
    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        
        // Reset previous results/errors
        hideResults();
        hideError();
        
        // Show loading state
        setLoadingState(true);
        
        try {
            console.log('Close to Pin form submission started');
            const formData = new FormData(form);
            console.log('FormData created');
            
            // Validate file upload
            console.log('Validating file upload...');
            const bestTimesFileElement = document.getElementById('best_times_file');
            
            if (!bestTimesFileElement) {
                throw new Error('File input element not found');
            }
            
            const bestTimesFile = bestTimesFileElement.files[0];
            
            if (!bestTimesFile) {
                throw new Error('Please upload the best times CSV file.');
            }
            console.log('File validation completed');
            
            console.log('Sending request to analyze close to pin...');
            const response = await fetch('/analyze-close-to-pin', {
                method: 'POST',
                body: formData
            });
            
            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.error || 'Failed to analyze close to pin data');
            }
            
            const result = await response.json();
            console.log('Analysis completed successfully');
            
            // Display results
            displayResults(result);
            
        } catch (error) {
            console.error('Error:', error);
            showError(error.message);
        } finally {
            setLoadingState(false);
        }
    });
} else {
    console.error('Form element not found - cannot attach event listener');
}

// Loading state management
function setLoadingState(loading) {
    if (!submitBtn || !btnText || !loadingSpinner) {
        console.warn('Button elements not found for loading state management');
        return;
    }
    
    if (loading) {
        submitBtn.disabled = true;
        btnText.style.display = 'none';
        loadingSpinner.style.display = 'flex';
    } else {
        submitBtn.disabled = false;
        btnText.style.display = 'block';
        loadingSpinner.style.display = 'none';
    }
}

// Results display
function displayResults(data) {
    const swimmers = data.swimmers;
    
    let html = `
        <div class="result-section">
            <h3>🎯 Close to Pin Analysis Results</h3>
            <p>Analyzed ${swimmers.length} swimmer records</p>
            
            <div class="table-controls">
                <div class="search-container">
                    <input type="text" id="swimmer-search" placeholder="🔍 Search swimmers, events, or times..." 
                           class="search-input">
                    <div class="search-stats">
                        <span id="search-results-count">${swimmers.length}</span> of ${swimmers.length} records shown
                    </div>
                </div>
            </div>
            
            <div class="table-container">
                <table class="results-table" id="swimmers-table">
                    <thead>
                        <tr>
                            <th>Last Name</th>
                            <th>First Name</th>
                            <th>Event</th>
                            <th>Best Time</th>
                            <th>Championship Meet</th>
                            <th>Gold Time</th>
                            <th>Silver Time</th>
                        </tr>
                    </thead>
                    <tbody id="swimmers-table-body">
    `;
    
    swimmers.forEach((swimmer, index) => {
        html += `
            <tr data-index="${index}">
                <td>${swimmer['Last Name'] || ''}</td>
                <td>${swimmer['First Name'] || ''}</td>
                <td>${swimmer['Event'] || ''}</td>
                <td>${swimmer['Best Time'] || ''}</td>
                <td class="${getQualificationClass(swimmer['Championship Meet'])}">${swimmer['Championship Meet'] || ''}</td>
                <td>${swimmer['Gold Time'] || ''}</td>
                <td>${swimmer['Silver Time'] || ''}</td>
            </tr>
        `;
    });
    
    html += `
                    </tbody>
                </table>
            </div>
        </div>
    `;
    
    resultsContent.innerHTML = html;
    
    // Store results for download and filtering
    window.closeToPinResults = data;
    window.allSwimmers = swimmers;
    
    // Setup search functionality
    setupSearchFilter();
    
    showResults();
}

// Setup search filter functionality
function setupSearchFilter() {
    const searchInput = document.getElementById('swimmer-search');
    const tableBody = document.getElementById('swimmers-table-body');
    const resultsCount = document.getElementById('search-results-count');
    
    if (!searchInput || !tableBody || !resultsCount) {
        console.warn('Search elements not found');
        return;
    }
    
    searchInput.addEventListener('input', function(e) {
        const searchTerm = e.target.value.toLowerCase().trim();
        const rows = tableBody.querySelectorAll('tr');
        let visibleCount = 0;
        
        rows.forEach(row => {
            const cells = row.querySelectorAll('td');
            let rowText = '';
            
            // Combine all cell text for searching
            cells.forEach(cell => {
                rowText += cell.textContent.toLowerCase() + ' ';
            });
            
            // Show/hide row based on search term
            if (searchTerm === '' || rowText.includes(searchTerm)) {
                row.style.display = '';
                visibleCount++;
            } else {
                row.style.display = 'none';
            }
        });
        
        // Update results count
        resultsCount.textContent = visibleCount;
        
        // Show message if no results
        const existingMessage = document.getElementById('no-results-message');
        if (visibleCount === 0 && searchTerm !== '') {
            if (!existingMessage) {
                const noResultsMessage = document.createElement('div');
                noResultsMessage.id = 'no-results-message';
                noResultsMessage.className = 'no-results-message';
                noResultsMessage.innerHTML = `
                    <p>No swimmers found matching "${searchTerm}"</p>
                    <small>Try searching by name, event, or qualification status</small>
                `;
                tableBody.parentNode.insertBefore(noResultsMessage, tableBody.nextSibling);
            }
        } else {
            if (existingMessage) {
                existingMessage.remove();
            }
        }
    });
    
    // Clear search functionality
    searchInput.addEventListener('keydown', function(e) {
        if (e.key === 'Escape') {
            e.target.value = '';
            e.target.dispatchEvent(new Event('input'));
        }
    });
}

// Helper function to style qualification status
function getQualificationClass(qualification) {
    if (!qualification) return '';
    if (qualification.includes('Gold')) return 'qualification-gold';
    if (qualification.includes('Silver')) return 'qualification-silver';
    if (qualification.includes('Bronze')) return 'qualification-bronze';
    return '';
}

// Download functionality
async function downloadResults(format) {
    if (!window.closeToPinResults) {
        showError('No results available for download');
        return;
    }
    
    try {
        const response = await fetch(`/download-close-to-pin/${format}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(window.closeToPinResults)
        });
        
        if (!response.ok) {
            throw new Error('Download failed');
        }
        
        // Get the filename from the Content-Disposition header or use the format-specific name
        let filename = `close_to_pin_analysis.${format}`;
        const contentDisposition = response.headers.get('Content-Disposition');
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename="?([^"]+)"?/);
            if (filenameMatch) {
                filename = filenameMatch[1];
            }
        }
        
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        window.URL.revokeObjectURL(url);
        
    } catch (error) {
        console.error('Download error:', error);
        showError('Failed to download file');
    }
}

// UI state management
function showResults() {
    if (resultsSection) {
        resultsSection.style.display = 'block';
        resultsSection.scrollIntoView({ behavior: 'smooth' });
    }
}

function hideResults() {
    if (resultsSection) {
        resultsSection.style.display = 'none';
    }
}

function showError(message) {
    if (errorContent && errorSection) {
        errorContent.textContent = message;
        errorSection.style.display = 'block';
        errorSection.scrollIntoView({ behavior: 'smooth' });
    } else {
        console.error('Error display elements not found:', message);
    }
}

function hideError() {
    if (errorSection) {
        errorSection.style.display = 'none';
    }
}

// Form reset handler
if (form) {
    form.addEventListener('reset', () => {
        hideResults();
        hideError();
    });
}

// File input validation
const bestTimesFileInput = document.getElementById('best_times_file');

if (bestTimesFileInput) {
    bestTimesFileInput.addEventListener('change', function(e) {
        validateCSVFile(e.target, 'best times');
    });
}

function validateCSVFile(input, type) {
    const file = input.files[0];
    if (!file) return true;
    
    // File extension validation
    if (!file.name.toLowerCase().endsWith('.csv')) {
        showError(`Please upload a valid CSV file for ${type}`);
        input.value = '';
        return false;
    }
    
    // File size validation (16MB max)
    const maxSize = 16 * 1024 * 1024;
    if (file.size > maxSize) {
        showError(`File too large. Maximum size is 16MB for ${type}`);
        input.value = '';
        return false;
    }
    
    // Filename validation (basic)
    const filename = file.name;
    if (filename.length > 255) {
        showError(`Filename too long for ${type}`);
        input.value = '';
        return false;
    }
    
    // Check for potentially dangerous characters
    const dangerousChars = /[<>:"/\\|?*\x00-\x1f]/;
    if (dangerousChars.test(filename)) {
        showError(`Invalid characters in filename for ${type}`);
        input.value = '';
        return false;
    }
    
    return true;
}

// Add custom CSS for qualification styling and search functionality
const style = document.createElement('style');
style.textContent = `
    .qualification-gold { 
        background: #fff3cd; 
        color: #856404; 
        font-weight: bold; 
        padding: 4px 8px; 
        border-radius: 4px; 
    }
    .qualification-silver { 
        background: #e2e3e5; 
        color: #383d41; 
        font-weight: bold; 
        padding: 4px 8px; 
        border-radius: 4px; 
    }
    .qualification-bronze { 
        background: #f8d7da; 
        color: #721c24; 
        font-weight: bold; 
        padding: 4px 8px; 
        border-radius: 4px; 
    }
    
    .table-controls {
        margin: 20px 0;
        padding: 20px;
        background: #f8f9fa;
        border-radius: 8px;
        border: 1px solid #e9ecef;
    }
    
    .search-container {
        display: flex;
        flex-direction: column;
        gap: 10px;
    }
    
    .search-input {
        width: 100%;
        padding: 12px 16px;
        font-size: 1rem;
        border: 2px solid #dee2e6;
        border-radius: 8px;
        background: white;
        transition: all 0.3s ease;
    }
    
    .search-input:focus {
        outline: none;
        border-color: #3498db;
        box-shadow: 0 0 0 3px rgba(52, 152, 219, 0.1);
    }
    
    .search-input::placeholder {
        color: #6c757d;
        font-style: italic;
    }
    
    .search-stats {
        font-size: 0.9rem;
        color: #6c757d;
        font-weight: 500;
    }
    
    .search-stats span {
        color: #495057;
        font-weight: 600;
    }
    
    .no-results-message {
        text-align: center;
        padding: 40px 20px;
        background: #f8f9fa;
        border: 2px dashed #dee2e6;
        border-radius: 8px;
        margin: 20px 0;
    }
    
    .no-results-message p {
        margin: 0 0 8px 0;
        font-weight: 600;
        color: #495057;
    }
    
    .no-results-message small {
        color: #6c757d;
        font-style: italic;
    }
    
    @media (max-width: 768px) {
        .search-input {
            font-size: 16px; /* Prevents zoom on iOS */
        }
        
        .table-controls {
            margin: 15px 0;
            padding: 15px;
        }
    }
`;
document.head.appendChild(style);

// Load standards modification date
async function loadStandardsInfo() {
    try {
        const response = await fetch('/standards-info');
        if (response.ok) {
            const data = await response.json();
            const dateElement = document.getElementById('standards-date');
            if (dateElement) {
                dateElement.textContent = data.last_modified;
            }
        } else {
            const dateElement = document.getElementById('standards-date');
            if (dateElement) {
                dateElement.textContent = 'Unknown';
            }
        }
    } catch (error) {
        console.error('Failed to load standards info:', error);
        const dateElement = document.getElementById('standards-date');
        if (dateElement) {
            dateElement.textContent = 'Unknown';
        }
    }
}