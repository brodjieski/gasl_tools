/**
 * PDF Labeler JavaScript
 * Handles the PDF labeling form submission and user interactions
 */

document.addEventListener('DOMContentLoaded', function() {
    // DOM elements
    const form = document.getElementById('pdf-labeler-form');
    const submitBtn = document.getElementById('submit-btn');
    const btnText = submitBtn.querySelector('.btn-text');
    const loadingSpinner = submitBtn.querySelector('.loading-spinner');
    const labelsInput = document.getElementById('labels_input');
    const presetButtons = document.querySelectorAll('.preset-btn');
    const successSection = document.getElementById('success-section');
    const errorSection = document.getElementById('error-section');
    const successContent = document.getElementById('success-content');
    const errorContent = document.getElementById('error-content');
    
    // Label setting elements
    const fontSizeInput = document.getElementById('font_size');
    const colorSelect = document.getElementById('color');
    const previewText = document.getElementById('preview-text');
    const labelPreview = document.getElementById('label-preview');

    // Preset button handlers
    presetButtons.forEach(button => {
        button.addEventListener('click', function() {
            const labels = this.getAttribute('data-labels');
            labelsInput.value = labels.replace(/,/g, '\n');
            
            // Add visual feedback
            this.style.backgroundColor = '#e8f5e8';
            setTimeout(() => {
                this.style.backgroundColor = '';
            }, 200);
        });
    });

    // Preview update handlers
    function updatePreview() {
        const fontSize = fontSizeInput.value || 10;
        const color = colorSelect.value || 'red';
        
        // Fixed settings
        const fontName = 'Helvetica-Bold';  // Fixed
        const uppercase = true;  // Fixed: always uppercase
        const positionTop = 20;  // Fixed: 20 points from top
        
        // Get sample text from labels input or use default
        let sampleText = 'SAMPLE LABEL';
        const labelsText = labelsInput.value.trim();
        if (labelsText) {
            const firstLabel = labelsText.split(/\n|,/)[0].trim();
            if (firstLabel) {
                sampleText = firstLabel.toUpperCase();  // Always uppercase
            }
        }
        
        // Update preview text
        previewText.textContent = sampleText;
        
        // Apply styles
        previewText.style.fontSize = fontSize + 'px';
        previewText.style.color = color;
        previewText.style.fontFamily = 'bold Arial, sans-serif';  // Fixed to Helvetica Bold equivalent
        
        // Update position indicator (fixed at 20 points)
        labelPreview.style.paddingTop = Math.max(10, positionTop / 2) + 'px';
    }

    // Add event listeners for preview updates
    [fontSizeInput, colorSelect, labelsInput].forEach(element => {
        element.addEventListener('input', updatePreview);
        element.addEventListener('change', updatePreview);
    });

    // Initial preview update
    updatePreview();

    // Form submission handler
    form.addEventListener('submit', async function(e) {
        e.preventDefault();
        
        // Hide previous results
        hideResults();
        
        // Validate form
        if (!validateForm()) {
            return;
        }
        
        // Show loading state
        setLoadingState(true);
        
        try {
            // Prepare form data
            const formData = new FormData();
            const pdfFile = document.getElementById('pdf_file').files[0];
            const labelsText = labelsInput.value.trim();
            
            formData.append('pdf_file', pdfFile);
            
            // Parse labels (split by newlines or commas)
            const labels = parseLabels(labelsText);
            formData.append('labels', JSON.stringify(labels));
            
            // Add label settings (only customizable ones)
            formData.append('font_size', fontSizeInput.value);
            formData.append('color', colorSelect.value);
            
            // Submit form
            const response = await fetch('/label-pdf', {
                method: 'POST',
                body: formData
            });
            
            if (response.ok) {
                // Success - handle file download
                const blob = await response.blob();
                const filename = getFilenameFromResponse(response) || 'labeled_document.pdf';
                
                // Create download link
                const url = window.URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = filename;
                
                // Show success message with download link
                showSuccess(filename, url);
                
                // Auto-download
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                
                // Clean up
                setTimeout(() => {
                    window.URL.revokeObjectURL(url);
                }, 1000);
                
            } else {
                // Error response
                const errorData = await response.json();
                showError(errorData.error || 'An error occurred while processing the PDF');
            }
            
        } catch (error) {
            console.error('Error:', error);
            showError('Network error: ' + error.message);
        } finally {
            setLoadingState(false);
        }
    });

    // Form reset handler
    form.addEventListener('reset', function() {
        hideResults();
        setTimeout(() => {
            labelsInput.focus();
        }, 100);
    });

    // File input change handler
    document.getElementById('pdf_file').addEventListener('change', function(e) {
        const file = e.target.files[0];
        if (file) {
            validateFile(file);
        }
    });

    /**
     * Validate the form before submission
     */
    function validateForm() {
        const pdfFile = document.getElementById('pdf_file').files[0];
        const labelsText = labelsInput.value.trim();
        
        // Check PDF file
        if (!pdfFile) {
            showError('Please select a PDF file');
            return false;
        }
        
        if (!validateFile(pdfFile)) {
            return false;
        }
        
        // Check labels
        if (!labelsText) {
            showError('Please enter at least one label');
            labelsInput.focus();
            return false;
        }
        
        const labels = parseLabels(labelsText);
        if (labels.length === 0) {
            showError('Please enter valid labels');
            labelsInput.focus();
            return false;
        }
        
        if (labels.length > 20) {
            showError('Maximum 20 labels allowed');
            labelsInput.focus();
            return false;
        }
        
        return true;
    }

    /**
     * Validate the selected file
     */
    function validateFile(file) {
        // Check file type
        if (file.type !== 'application/pdf') {
            showError('Please select a PDF file');
            return false;
        }
        
        // Check file size (16MB max)
        const maxSize = 16 * 1024 * 1024;
        if (file.size > maxSize) {
            showError('File size must be less than 16MB');
            return false;
        }
        
        return true;
    }

    /**
     * Parse labels from text input
     */
    function parseLabels(text) {
        // Split by newlines first, then by commas
        let labels = text.split(/\n|,/).map(label => label.trim()).filter(label => label.length > 0);
        
        // Remove duplicates
        labels = [...new Set(labels)];
        
        // Validate each label
        labels = labels.filter(label => {
            // Check length
            if (label.length > 50) {
                return false;
            }
            
            // Check for invalid characters (basic validation)
            if (/[<>\"'&]/.test(label)) {
                return false;
            }
            
            return true;
        });
        
        return labels;
    }

    /**
     * Extract filename from response headers
     */
    function getFilenameFromResponse(response) {
        const contentDisposition = response.headers.get('Content-Disposition');
        if (contentDisposition) {
            const match = contentDisposition.match(/filename="(.+)"/);
            if (match) {
                return match[1];
            }
        }
        return null;
    }

    /**
     * Set loading state
     */
    function setLoadingState(isLoading) {
        if (isLoading) {
            submitBtn.disabled = true;
            btnText.style.display = 'none';
            loadingSpinner.style.display = 'inline-flex';
        } else {
            submitBtn.disabled = false;
            btnText.style.display = 'inline';
            loadingSpinner.style.display = 'none';
        }
    }

    /**
     * Show success message
     */
    function showSuccess(filename, downloadUrl) {
        hideResults();
        
        const downloadLink = document.getElementById('download-link');
        downloadLink.href = downloadUrl;
        downloadLink.download = filename;
        
        successContent.innerHTML = `
            <p>✅ Your labeled PDF has been generated successfully!</p>
            <p><strong>File:</strong> ${filename}</p>
            <p>The download should start automatically. If not, <a href="${downloadUrl}" download="${filename}">click here to download</a>.</p>
        `;
        
        successSection.style.display = 'block';
        successSection.scrollIntoView({ behavior: 'smooth' });
    }

    /**
     * Show error message
     */
    function showError(message) {
        hideResults();
        
        errorContent.innerHTML = `
            <p>❌ ${message}</p>
            <p>Please check your input and try again.</p>
        `;
        
        errorSection.style.display = 'block';
        errorSection.scrollIntoView({ behavior: 'smooth' });
    }

    /**
     * Hide all result sections
     */
    function hideResults() {
        successSection.style.display = 'none';
        errorSection.style.display = 'none';
    }

    // Auto-focus on labels input
    if (labelsInput) {
        labelsInput.focus();
    }
});