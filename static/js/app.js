// DOM elements
console.log('Loading DOM elements...');
const form = document.getElementById('standards-form');

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
        console.log('Form submission started');
        const formData = new FormData(form);
        console.log('FormData created');
        
        // Get selected analysis options
        console.log('Getting analysis options...');
        const calculateStandards = document.getElementById('calculate_standards').checked;
        const analyzeCurrent = document.getElementById('analyze_current').checked;
        const estimateDuration = document.getElementById('estimate_duration').checked;
        console.log('Analysis options:', { calculateStandards, analyzeCurrent, estimateDuration });
        
        // Validate that at least one option is selected
        if (!calculateStandards && !analyzeCurrent && !estimateDuration) {
            throw new Error('Please select at least one analysis option.');
        }
        
        // Validate file uploads
        const dataFiles = document.getElementById('data_files').files;
        
        if (dataFiles.length === 0) {
            throw new Error('Please upload at least one swimmer data CSV file.');
        }
        
        const results = {};
        
        // Execute selected analyses
        if (calculateStandards) {
            console.log('Calculating new standards...');
            const standardsResult = await fetch('/calculate-standards', {
                method: 'POST',
                body: formData
            });
            
            if (!standardsResult.ok) {
                const error = await standardsResult.json();
                throw new Error(error.error || 'Failed to calculate standards');
            }
            
            results.standards = await standardsResult.json();
        }
        
        if (analyzeCurrent) {
            console.log('Analyzing current standards...');
            const analysisResult = await fetch('/analyze-current-standards', {
                method: 'POST',
                body: formData
            });
            
            if (!analysisResult.ok) {
                const error = await analysisResult.json();
                throw new Error(error.error || 'Failed to analyze current standards');
            }
            
            results.analysis = await analysisResult.json();
        }
        
        if (estimateDuration) {
            console.log('Estimating meet duration...');
            const durationResult = await fetch('/estimate-meet-duration', {
                method: 'POST',
                body: formData
            });
            
            if (!durationResult.ok) {
                const error = await durationResult.json();
                throw new Error(error.error || 'Failed to estimate meet duration');
            }
            
            results.duration = await durationResult.json();
        }
        
        // Display results
        displayResults(results);
        
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
function displayResults(results) {
    let html = '';
    
    if (results.standards) {
        html += createStandardsResultsHTML(results.standards);
    }
    
    if (results.analysis) {
        html += createAnalysisResultsHTML(results.analysis);
    }
    
    if (results.duration) {
        html += createDurationResultsHTML(results.duration);
    }
    
    resultsContent.innerHTML = html;
    showResults();
}

function createStandardsResultsHTML(data) {
    const parameters = data.parameters;
    const results = data.results;
    
    let html = `
        <div class="result-section">
            <h3>📊 New Time Standards</h3>
            <div class="parameters-info">
                <p><strong>Gold Percentile:</strong> ${(parameters.gold_percentile * 100).toFixed(1)}%</p>
                <p><strong>Silver Percentile:</strong> ${(parameters.silver_percentile * 100).toFixed(1)}%</p>
            </div>
            
            <div class="standards-actions">
                <div class="action-buttons">
                    <button onclick="openReviewModal()" id="review-btn" class="btn-secondary">
                        ✏️ Review & Edit Standards
                    </button>
                </div>
                
                <div class="download-buttons">
                    <button onclick="downloadResults('csv', 'time_standards')" class="download-btn">
                        📁 Download Analysis CSV
                    </button>
                    <button onclick="downloadResults('standards_csv', 'time_standards')" class="download-btn">
                        📄 Download Standards CSV
                    </button>
                    <button onclick="downloadResults('xlsx', 'time_standards')" class="download-btn">
                        📊 Download Excel
                    </button>
                </div>
            </div>
            
            <div class="table-container">
                <table class="results-table" id="standards-table">
                    <thead>
                        <tr>
                            <th>Event ID</th>
                            <th>Event</th>
                            <th>Current Gold (yd)</th>
                            <th>Proposed Gold (yd)</th>
                            <th>Gold Delta (yd)</th>
                            <th>Current Silver (yd)</th>
                            <th>Proposed Silver (yd)</th>
                            <th>Silver Delta (yd)</th>
                        </tr>
                    </thead>
                    <tbody>
    `;
    
    results.forEach((row, index) => {
        html += `
            <tr data-index="${index}">
                <td>${row['Event_ID'] || ''}</td>
                <td>${row['Event'] || ''}</td>
                <td>${row['Current Gold Time (yards)'] || ''}</td>
                <td class="proposed-value" data-field="gold_y" data-original="${row['Proposed Gold Time (yards)']}">${row['Proposed Gold Time (yards)'] || ''}</td>
                <td class="${getTimeChangeClass(row['Gold delta (yards)'])}">${row['Gold delta (yards)'] || ''}</td>
                <td>${row['Current Silver Time (yards)'] || ''}</td>
                <td class="proposed-value" data-field="silver_y" data-original="${row['Proposed Silver Time (yards)']}">${row['Proposed Silver Time (yards)'] || ''}</td>
                <td class="${getTimeChangeClass(row['Silver delta (yards)'])}">${row['Silver delta (yards)'] || ''}</td>
            </tr>
        `;
    });
    
    html += `
                    </tbody>
                </table>
            </div>
            
        </div>
    `;
    
    // Store results for download and editing
    window.standardsResults = data;
    window.originalResults = JSON.parse(JSON.stringify(data)); // Deep copy
    
    return html;
}

function createAnalysisResultsHTML(data) {
    const analysis = data.analysis;
    
    let html = `
        <div class="result-section">
            <h3>🔍 Current Standards Analysis</h3>
            <p>Analysis of where current time standards fall in the performance distribution:</p>
            
            <div class="table-container">
                <table class="results-table">
                    <thead>
                        <tr>
                            <th>Event</th>
                            <th>Current Gold Percentile</th>
                            <th>Current Silver Percentile</th>
                        </tr>
                    </thead>
                    <tbody>
    `;
    
    analysis.forEach(row => {
        html += `
            <tr>
                <td>${row['Event_name'] || ''}</td>
                <td>${row['current_gold_percentile'] ? row['current_gold_percentile'].toFixed(1) + '%' : ''}</td>
                <td>${row['current_silver_percentile'] ? row['current_silver_percentile'].toFixed(1) + '%' : ''}</td>
            </tr>
        `;
    });
    
    html += `
                    </tbody>
                </table>
            </div>
        </div>
    `;
    
    return html;
}

function createDurationResultsHTML(data) {
    const parameters = data.parameters;
    const estimates = data.duration_estimates;
    
    let html = `
        <div class="result-section">
            <h3>⏱️ Meet Duration Estimates</h3>
            <div class="parameters-info">
                <p><strong>Heat Time:</strong> ${parameters.heat_time} seconds</p>
                <p><strong>Event Time:</strong> ${parameters.event_time} seconds</p>
            </div>
    `;
    
    estimates.forEach(seasonData => {
        html += `
            <div class="season-estimate">
                <h4>Season ${seasonData.season}</h4>
                
                <div class="summary-cards">
                    <div class="summary-card gold">
                        <h5>Gold Meet</h5>
                        <p class="duration">${formatDuration(calculateTotalDuration(seasonData.times_df, seasonData.season, 'gold'))}</p>
                        <p class="details">${getTotalQualifiers(seasonData.times_df, seasonData.season, 'gold')} total entries</p>
                    </div>
                    <div class="summary-card silver">
                        <h5>Silver Meet (each)</h5>
                        <p class="duration">${formatDuration(calculateTotalDuration(seasonData.times_df, seasonData.season, 'silver') / 2)}</p>
                        <p class="details">${Math.round(getTotalQualifiers(seasonData.times_df, seasonData.season, 'silver') / 2)} entries per meet</p>
                    </div>
                    <div class="summary-card bronze">
                        <h5>Bronze Meet (each)</h5>
                        <p class="duration">${formatDuration(calculateTotalDuration(seasonData.times_df, seasonData.season, 'bronze') / 2)}</p>
                        <p class="details">${Math.round(getTotalQualifiers(seasonData.times_df, seasonData.season, 'bronze') / 2)} entries per meet</p>
                    </div>
                </div>
                
                <h5>Team Attendance - New Standards</h5>
                ${createAttendanceTable(seasonData.new_entries_summary)}
                
                <h5>Team Attendance - Current Standards</h5>
                ${createAttendanceTable(seasonData.current_entries_summary)}
            </div>
        `;
    });
    
    html += '</div>';
    
    return html;
}

function createAttendanceTable(attendanceData) {
    let html = `
        <div class="table-container">
            <table class="results-table attendance-table">
                <thead>
                    <tr>
                        <th>Team</th>
                        <th>Gold</th>
                        <th>Silver</th>
                        <th>Bronze</th>
                        <th>Total</th>
                    </tr>
                </thead>
                <tbody>
    `;
    
    Object.entries(attendanceData).forEach(([team, counts]) => {
        if (team !== 'Total') {
            html += `
                <tr>
                    <td>${team}</td>
                    <td>${counts.GOLD || 0}</td>
                    <td>${counts.SILVER || 0}</td>
                    <td>${counts.BRONZE || 0}</td>
                    <td>${counts.Total || 0}</td>
                </tr>
            `;
        }
    });
    
    // Add total row
    if (attendanceData.Total) {
        html += `
            <tr class="total-row">
                <td><strong>Total</strong></td>
                <td><strong>${attendanceData.Total.GOLD || 0}</strong></td>
                <td><strong>${attendanceData.Total.SILVER || 0}</strong></td>
                <td><strong>${attendanceData.Total.BRONZE || 0}</strong></td>
                <td><strong>${attendanceData.Total.Total || 0}</strong></td>
            </tr>
        `;
    }
    
    html += `
                </tbody>
            </table>
        </div>
    `;
    
    return html;
}

// Helper functions
function getTimeChangeClass(delta) {
    if (!delta || delta === '') return '';
    if (delta.startsWith('-')) return 'time-faster';
    return 'time-slower';
}

function calculateTotalDuration(timesData, season, meetType) {
    return timesData.reduce((total, row) => {
        const duration = row[`${meetType}_est_duration-${season}`] || 0;
        return total + duration;
    }, 0) + (meetType === 'gold' ? 12000 : 24000); // Add relay time
}

function getTotalQualifiers(timesData, season, meetType) {
    return timesData.reduce((total, row) => {
        const qualifiers = row[`${meetType}_qualifiers-${season}`] || 0;
        return total + qualifiers;
    }, 0);
}

function formatDuration(hundredths) {
    const totalSeconds = hundredths / 100;
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.floor((totalSeconds % 3600) / 60);
    
    if (hours > 0) {
        return `${hours}h ${minutes}m`;
    } else {
        return `${minutes}m`;
    }
}

// Download functionality
async function downloadResults(format, type) {
    if (!window.standardsResults) {
        showError('No results available for download');
        return;
    }
    
    try {
        const response = await fetch(`/download-standards/${format}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(window.standardsResults)
        });
        
        if (!response.ok) {
            throw new Error('Download failed');
        }
        
        // Get the filename from the Content-Disposition header or use the format-specific name
        let filename = `time_standards.${format}`;
        const contentDisposition = response.headers.get('Content-Disposition');
        if (contentDisposition) {
            const filenameMatch = contentDisposition.match(/filename="?([^"]+)"?/);
            if (filenameMatch) {
                filename = filenameMatch[1];
            }
        } else {
            // Use format-specific filenames
            if (format === 'standards_csv') {
                filename = 'new_time_standards.csv';
            } else if (format === 'csv') {
                filename = 'time_standards.csv';
            } else if (format === 'xlsx') {
                filename = 'time_standards.xlsx';
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
    resultsSection.style.display = 'block';
    resultsSection.scrollIntoView({ behavior: 'smooth' });
}

function hideResults() {
    resultsSection.style.display = 'none';
}

function showError(message) {
    errorContent.textContent = message;
    errorSection.style.display = 'block';
    errorSection.scrollIntoView({ behavior: 'smooth' });
}

function hideError() {
    errorSection.style.display = 'none';
}

// Form reset handler
form.addEventListener('reset', () => {
    hideResults();
    hideError();
});

// File input validation
document.getElementById('data_files').addEventListener('change', function(e) {
    for (let file of e.target.files) {
        validateCSVFile({ files: [file] }, 'swimmer data');
    }
});

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

// Interactive Standards Review Functions
function openReviewModal() {
    if (!window.standardsResults) {
        alert('No standards data available for review');
        return;
    }
    
    const modal = createReviewModal();
    document.body.appendChild(modal);
    document.body.style.overflow = 'hidden'; // Prevent background scrolling
    
    // Initialize the select all checkbox state after modal is added to DOM
    setTimeout(() => {
        updateSelectAllCheckbox();
    }, 0);
}

function createReviewModal() {
    const results = window.standardsResults.results;
    const parameters = window.standardsResults.parameters;
    
    const modal = document.createElement('div');
    modal.className = 'review-modal';
    modal.id = 'review-modal';
    
    modal.innerHTML = `
        <div class="modal-content">
            <div class="modal-header">
                <h3>📊 Review & Edit Time Standards</h3>
                <button class="modal-close" onclick="closeReviewModal()">&times;</button>
            </div>
            
            <div class="modal-body">
                <div class="modal-info">
                    <p><strong>Gold Percentile:</strong> ${(parameters.gold_percentile * 100).toFixed(1)}% | 
                       <strong>Silver Percentile:</strong> ${(parameters.silver_percentile * 100).toFixed(1)}%</p>
                </div>
                
                <div class="modal-actions">
                    <label class="select-all-container">
                        <input type="checkbox" id="select-all-checkbox" onchange="toggleAllStandards()" checked>
                        Accept All Proposed Changes
                    </label>
                    
                    <div class="action-buttons">
                        <button onclick="saveStandards()" class="btn-success">
                            💾 Save Final Standards
                        </button>
                        <button onclick="closeReviewModal()" class="btn-secondary">
                            Cancel
                        </button>
                    </div>
                </div>
                
                <div class="modal-table-container">
                    <table class="review-table">
                        <thead>
                            <tr>
                                <th>Accept</th>
                                <th>Event</th>
                                <th>Current Gold</th>
                                <th>Proposed Gold</th>
                                <th>Current Silver</th>
                                <th>Proposed Silver</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${createReviewTableRows(results)}
                        </tbody>
                    </table>
                </div>
                
                <div class="modal-instructions">
                    <p><strong>Instructions:</strong> Check events to accept proposed changes. Click on proposed times to edit manually. Unchecked events will keep current standards.</p>
                </div>
            </div>
        </div>
    `;
    
    return modal;
}

function createReviewTableRows(results) {
    return results.map((row, index) => `
        <tr data-index="${index}" class="review-row">
            <td class="checkbox-cell">
                <label class="checkbox-container">
                    <input type="checkbox" class="row-checkbox" data-index="${index}" onchange="toggleRowStatus(${index})" checked>
                </label>
            </td>
            <td class="event-cell">${row['Event'] || ''}</td>
            <td class="current-time">${row['Current Gold Time (yards)'] || ''}</td>
            <td class="proposed-time editable" data-field="gold_y" data-index="${index}" onclick="makeTimeEditable(this)">${row['Proposed Gold Time (yards)'] || ''}</td>
            <td class="current-time">${row['Current Silver Time (yards)'] || ''}</td>
            <td class="proposed-time editable" data-field="silver_y" data-index="${index}" onclick="makeTimeEditable(this)">${row['Proposed Silver Time (yards)'] || ''}</td>
        </tr>
    `).join('');
}

function makeTimeEditable(cell) {
    if (cell.querySelector('input')) return; // Already editing
    
    const currentValue = cell.textContent;
    const input = document.createElement('input');
    input.type = 'text';
    input.value = currentValue;
    input.className = 'time-input';
    
    const saveEdit = () => {
        const inputValue = input.value.trim();
        const formattedTime = formatTimeInput(inputValue);
        
        if (formattedTime !== null) {
            cell.textContent = formattedTime;
            // Update the stored data
            updateStoredValue(cell.dataset.index, cell.dataset.field, formattedTime);
            
            // Check if the value was actually changed
            if (formattedTime !== currentValue) {
                // Mark the row as modified
                const row = cell.closest('tr');
                row.classList.add('row-modified');
            }
        } else {
            // Invalid input - keep original value
            cell.textContent = currentValue;
        }
    };
    
    input.addEventListener('blur', saveEdit);
    input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            saveEdit();
        }
        if (e.key === 'Escape') {
            cell.textContent = currentValue;
        }
    });
    
    cell.innerHTML = '';
    cell.appendChild(input);
    input.focus();
    input.select();
}

function updateStoredValue(rowIndex, field, newValue) {
    const fieldMap = {
        'gold_y': 'Proposed Gold Time (yards)',
        'silver_y': 'Proposed Silver Time (yards)'
    };
    
    if (window.standardsResults && window.standardsResults.results[rowIndex]) {
        window.standardsResults.results[rowIndex][fieldMap[field]] = newValue;
    }
}

function toggleRowStatus(index) {
    const checkbox = document.querySelector(`input.row-checkbox[data-index="${index}"]`);
    
    console.log(`Toggle row ${index}: checkbox checked =`, checkbox?.checked);
    
    if (!checkbox) {
        console.error(`Could not find checkbox for index ${index}`);
        return;
    }
    
    // Only update select all checkbox if this is an individual toggle, not part of toggle all
    if (!window.isTogglingAll) {
        updateSelectAllCheckbox();
    }
}

function toggleAllStandards() {
    const selectAllCheckbox = document.getElementById('select-all-checkbox');
    const rowCheckboxes = document.querySelectorAll('.row-checkbox');
    
    console.log('Toggle all - selectAll checked:', selectAllCheckbox.checked);
    console.log('Found row checkboxes:', rowCheckboxes.length);
    
    // Set flag to prevent recursive updateSelectAllCheckbox calls
    window.isTogglingAll = true;
    
    // Clear indeterminate state first
    selectAllCheckbox.indeterminate = false;
    
    // Convert NodeList to Array to ensure proper iteration
    Array.from(rowCheckboxes).forEach((checkbox, index) => {
        console.log(`Setting checkbox ${index} to:`, selectAllCheckbox.checked);
        checkbox.checked = selectAllCheckbox.checked;
        toggleRowStatus(index);
    });
    
    // Clear the flag
    window.isTogglingAll = false;
    
    console.log('Finished toggling all checkboxes');
}

function updateSelectAllCheckbox() {
    const selectAllCheckbox = document.getElementById('select-all-checkbox');
    if (!selectAllCheckbox) return;
    
    const rowCheckboxes = document.querySelectorAll('.row-checkbox');
    const checkedBoxes = document.querySelectorAll('.row-checkbox:checked');
    
    console.log(`Update select all: ${checkedBoxes.length}/${rowCheckboxes.length} checked`);
    
    if (checkedBoxes.length === rowCheckboxes.length && rowCheckboxes.length > 0) {
        // All checked
        selectAllCheckbox.checked = true;
        selectAllCheckbox.indeterminate = false;
    } else if (checkedBoxes.length === 0) {
        // None checked
        selectAllCheckbox.checked = false;
        selectAllCheckbox.indeterminate = false;
    } else {
        // Some checked (indeterminate)
        selectAllCheckbox.checked = false;
        selectAllCheckbox.indeterminate = true;
    }
}

function closeReviewModal() {
    const modal = document.getElementById('review-modal');
    if (modal) {
        document.body.removeChild(modal);
        document.body.style.overflow = ''; // Restore scrolling
    }
}


function validateTimeFormat(timeStr) {
    // Try to parse and format the input
    const formatted = formatTimeInput(timeStr);
    return formatted !== null;
}

function formatTimeInput(input) {
    // Remove any non-digit characters
    const digits = input.replace(/\D/g, '');
    
    // Must have at least 3 digits and at most 6 digits
    if (digits.length < 3 || digits.length > 6) {
        return null;
    }
    
    // Pad with leading zeros to make it 6 digits (MMSSFF)
    const paddedDigits = digits.padStart(6, '0');
    
    // Extract MM, SS, HH (hundredths)
    const minutes = paddedDigits.substring(0, 2);
    const seconds = paddedDigits.substring(2, 4);
    const hundredths = paddedDigits.substring(4, 6);
    
    // Validate ranges: minutes 0-59, seconds 0-59, hundredths 0-99
    const min = parseInt(minutes);
    const sec = parseInt(seconds);
    const hun = parseInt(hundredths);
    
    if (min > 59 || sec > 59) {
        return null;
    }
    
    // Format as MM:SS.HH
    return `${minutes}:${seconds}.${hundredths}`;
}


async function saveStandards() {
    try {
        // Collect final standards data from modal
        const finalStandards = collectFinalStandardsFromModal();
        
        if (finalStandards.filter(s => s.Status === 'accepted').length === 0) {
            alert('Please select at least one standard to save.');
            return;
        }
        
        const response = await fetch('/save-final-standards', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                standards: finalStandards,
                parameters: window.standardsResults.parameters
            })
        });
        
        if (!response.ok) {
            throw new Error('Failed to save standards');
        }
        
        const result = await response.json();
        alert(`Standards saved successfully!\n\nAccepted: ${result.accepted_count}\nRejected: ${result.rejected_count}\nFile: ${result.new_file}`);
        
        // Update the main table with the final values
        updateMainTableWithFinalValues(finalStandards);
        
        closeReviewModal();
        
    } catch (error) {
        console.error('Save error:', error);
        alert('Failed to save standards: ' + error.message);
    }
}

function collectFinalStandardsFromModal() {
    const rows = document.querySelectorAll('.review-row');
    const finalStandards = [];
    
    rows.forEach(row => {
        const rowIndex = parseInt(row.dataset.index);
        const rowData = window.standardsResults.results[rowIndex];
        const checkbox = row.querySelector('.row-checkbox');
        const goldCell = row.querySelector('[data-field="gold_y"]');
        const silverCell = row.querySelector('[data-field="silver_y"]');
        
        finalStandards.push({
            ...rowData,
            'Proposed Gold Time (yards)': goldCell.textContent,
            'Proposed Silver Time (yards)': silverCell.textContent,
            'Final Gold Time (yards)': goldCell.textContent,
            'Final Silver Time (yards)': silverCell.textContent,
            'Status': checkbox.checked ? 'accepted' : 'rejected'
        });
    });
    
    return finalStandards;
}

function updateMainTableWithFinalValues(finalStandards) {
    // Update the stored results with the final values
    finalStandards.forEach((finalRow, index) => {
        if (finalRow.Status === 'accepted') {
            // Update the stored data
            window.standardsResults.results[index]['Proposed Gold Time (yards)'] = finalRow['Final Gold Time (yards)'];
            window.standardsResults.results[index]['Proposed Silver Time (yards)'] = finalRow['Final Silver Time (yards)'];
        } else {
            // For rejected rows, revert to current standards
            window.standardsResults.results[index]['Proposed Gold Time (yards)'] = window.standardsResults.results[index]['Current Gold Time (yards)'];
            window.standardsResults.results[index]['Proposed Silver Time (yards)'] = window.standardsResults.results[index]['Current Silver Time (yards)'];
        }
    });
    
    // Find and update the main results table
    const mainTable = document.getElementById('standards-table');
    if (mainTable) {
        const rows = mainTable.querySelectorAll('tbody tr');
        
        rows.forEach((row, index) => {
            const finalRow = finalStandards[index];
            if (finalRow) {
                // Update proposed Gold time (column index 3)
                const goldCell = row.cells[3];
                if (goldCell) {
                    if (finalRow.Status === 'accepted') {
                        goldCell.textContent = finalRow['Final Gold Time (yards)'];
                    } else {
                        // For rejected rows, keep the proposed time (it will be crossed out via CSS)
                        goldCell.textContent = finalRow['Proposed Gold Time (yards)'];
                    }
                }
                
                // Update proposed Silver time (column index 6)
                const silverCell = row.cells[6];
                if (silverCell) {
                    if (finalRow.Status === 'accepted') {
                        silverCell.textContent = finalRow['Final Silver Time (yards)'];
                    } else {
                        // For rejected rows, keep the proposed time (it will be crossed out via CSS)
                        silverCell.textContent = finalRow['Proposed Silver Time (yards)'];
                    }
                }
                
                // Update delta columns if needed (could recalculate here)
                // For now, just mark cells that were changed
                if (finalRow.Status === 'rejected') {
                    row.classList.add('row-reverted');
                } else if (finalRow['Final Gold Time (yards)'] !== finalRow['Proposed Gold Time (yards)'] || 
                          finalRow['Final Silver Time (yards)'] !== finalRow['Proposed Silver Time (yards)']) {
                    row.classList.add('row-manually-edited');
                }
            }
        });
    }
}

// Add custom CSS for time changes and summary cards
const style = document.createElement('style');
style.textContent = `
    .time-faster { color: #27ae60; font-weight: bold; }
    .time-slower { color: #e74c3c; font-weight: bold; }
    
    .result-section {
        margin-bottom: 40px;
        padding-bottom: 30px;
        border-bottom: 1px solid #e1e8ed;
    }
    
    .result-section:last-child {
        border-bottom: none;
    }
    
    .parameters-info {
        background: #f8f9fa;
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 20px;
    }
    
    .parameters-info p {
        margin: 5px 0;
        font-weight: 500;
    }
    
    .season-estimate {
        margin-bottom: 30px;
        padding: 20px;
        background: #f8f9fa;
        border-radius: 8px;
    }
    
    .summary-cards {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 20px;
        margin: 20px 0;
    }
    
    .summary-card {
        background: white;
        padding: 20px;
        border-radius: 8px;
        text-align: center;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        border-left: 4px solid;
    }
    
    .summary-card.gold { border-left-color: #f1c40f; }
    .summary-card.silver { border-left-color: #95a5a6; }
    .summary-card.bronze { border-left-color: #cd7f32; }
    
    .summary-card h5 {
        margin: 0 0 10px 0;
        color: #2c3e50;
    }
    
    .summary-card .duration {
        font-size: 1.8rem;
        font-weight: bold;
        margin: 10px 0;
        color: #2c3e50;
    }
    
    .summary-card .details {
        color: #7f8c8d;
        font-size: 0.9rem;
        margin: 0;
    }
    
    .attendance-table .total-row {
        background: #f8f9fa;
        border-top: 2px solid #3498db;
    }
    
    .table-container {
        overflow-x: auto;
        margin: 20px 0;
    }
    
    /* Interactive Standards Styling */
    .standards-actions {
        background: #f8f9fa;
        padding: 20px;
        border-radius: 8px;
        margin-bottom: 20px;
    }
    
    .action-buttons {
        display: flex;
        gap: 10px;
        margin-bottom: 15px;
        flex-wrap: wrap;
    }
    
    .btn-success {
        background: #28a745;
        color: white;
        border: none;
        padding: 10px 20px;
        border-radius: 5px;
        cursor: pointer;
        font-weight: 600;
    }
    
    .btn-success:hover {
        background: #218838;
    }
    
    .proposed-value.editable {
        background: #fff3cd;
        cursor: pointer;
        border: 2px dashed #ffc107;
        padding: 4px;
    }
    
    .proposed-value.editable:hover {
        background: #ffeaa7;
    }
    
    .time-input {
        width: 100%;
        border: 2px solid #007bff;
        padding: 4px;
        font-family: inherit;
        font-size: inherit;
        text-align: center;
    }
    
    .accept-btn, .reject-btn {
        background: none;
        border: none;
        font-size: 16px;
        cursor: pointer;
        padding: 4px;
        margin: 0 2px;
    }
    
    .accept-btn:hover {
        background: #d4edda;
        border-radius: 4px;
    }
    
    .reject-btn:hover {
        background: #f8d7da;
        border-radius: 4px;
    }
    
    .row-accepted {
        background: #d4edda !important;
    }
    
    .row-rejected {
        background: #f8d7da !important;
    }
    
    .row-modified {
        background: #fff3cd !important;
    }
    
    .edit-instructions {
        background: #e7f3ff;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #007bff;
        margin-top: 20px;
    }
    
    .edit-instructions ul {
        margin: 10px 0 0 0;
        padding-left: 20px;
    }
    
    .edit-instructions li {
        margin-bottom: 5px;
    }
    
    /* Modal Styles */
    .review-modal {
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        background: rgba(0, 0, 0, 0.5);
        display: flex;
        justify-content: center;
        align-items: center;
        z-index: 1000;
    }
    
    .modal-content {
        background: white;
        border-radius: 8px;
        width: 95%;
        max-width: 1200px;
        max-height: 90vh;
        display: flex;
        flex-direction: column;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.3);
    }
    
    .modal-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 20px;
        border-bottom: 1px solid #e9ecef;
        background: #f8f9fa;
        border-radius: 8px 8px 0 0;
    }
    
    .modal-header h3 {
        margin: 0;
        color: #2c3e50;
    }
    
    .modal-close {
        background: none;
        border: none;
        font-size: 24px;
        cursor: pointer;
        color: #6c757d;
        padding: 0;
        width: 30px;
        height: 30px;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    
    .modal-close:hover {
        color: #495057;
        background: #e9ecef;
        border-radius: 4px;
    }
    
    .modal-body {
        padding: 20px;
        flex: 1;
        overflow-y: auto;
    }
    
    .modal-info {
        background: #e7f3ff;
        padding: 10px 15px;
        border-radius: 6px;
        margin-bottom: 20px;
        border-left: 4px solid #007bff;
    }
    
    .modal-info p {
        margin: 0;
        font-weight: 500;
    }
    
    .modal-actions {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 20px;
        flex-wrap: wrap;
        gap: 15px;
    }
    
    .review-modal .select-all-container {
        display: flex;
        align-items: center;
        cursor: pointer;
        font-weight: 600;
        gap: 8px;
    }
    
    .review-modal .checkbox-container {
        display: flex;
        align-items: center;
        cursor: pointer;
        gap: 8px;
    }
    
    .review-modal .checkbox-container input[type="checkbox"] {
        width: 16px;
        height: 16px;
        margin: 0;
        cursor: pointer;
        accent-color: #007bff;
    }
    
    .review-modal .checkmark {
        display: none;
    }
    
    /* Fix for Analysis Options checkboxes */
    input[type="checkbox"] {
        width: 16px;
        height: 16px;
        margin: 0;
        vertical-align: middle;
        accent-color: #007bff;
    }
    
    .modal-table-container {
        max-height: 400px;
        overflow-y: auto;
        border: 1px solid #dee2e6;
        border-radius: 6px;
    }
    
    .review-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 13px;
    }
    
    .review-table th,
    .review-table td {
        padding: 6px 8px;
        text-align: left;
        border-bottom: 1px solid #e9ecef;
    }
    
    .review-table th {
        background: #f8f9fa;
        font-weight: 600;
        position: sticky;
        top: 0;
        z-index: 10;
    }
    
    .review-table .checkbox-cell {
        width: 60px;
        text-align: center;
    }
    
    .review-table .event-cell {
        min-width: 200px;
        font-weight: 500;
    }
    
    .review-table .current-time {
        color: #6c757d;
        font-family: inherit;
    }
    
    .review-table .proposed-time {
        font-family: inherit;
        font-weight: 600;
        background: #fff3cd;
        cursor: pointer;
        border-radius: 3px;
        padding: 4px 6px;
    }
    
    .review-table .proposed-time:hover {
        background: #ffeaa7;
    }
    
    .review-row.row-modified {
        background: #fff3cd;
    }
    
    .modal-instructions {
        background: #f8f9fa;
        padding: 12px 15px;
        border-radius: 6px;
        margin-top: 20px;
        font-size: 14px;
    }
    
    .modal-instructions p {
        margin: 0;
    }
    
    .time-input {
        width: 100%;
        border: 2px solid #007bff;
        padding: 2px 4px;
        font-family: inherit;
        font-size: 13px;
        text-align: center;
        border-radius: 3px;
    }
    
    /* Main table styling for updated rows */
    .row-manually-edited {
        background: #fff3cd !important;
        border-left: 4px solid #ffc107;
    }
    
    .row-reverted {
        background: #f8d7da !important;
        border-left: 4px solid #dc3545;
    }
    
    .row-manually-edited .proposed-value {
        font-weight: bold;
        color: #856404;
    }
    
    .row-reverted .proposed-value {
        text-decoration: line-through;
        color: #721c24;
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