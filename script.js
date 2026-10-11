console.log("script.js is loaded and running!");

document.addEventListener('DOMContentLoaded', () => {
    const form = document.getElementById('bogoForm');
    
    if (!form) {
        console.error("Could not find bogoForm in the DOM!");
        return;
    }

    // Listen for the form submission
    form.addEventListener('submit', async function(event) {
        // Prevents the page from resetting/refreshing
        event.preventDefault();
        await searchBOGOs();
    });
});

// Async function to handle the request
async function searchBOGOs() {
    const zipInput = document.getElementById('zipInput');
    const zipCode = zipInput ? zipInput.value.trim() : '';

    const resultsDiv = document.getElementById('results');
    resultsDiv.style.display = 'block';
    resultsDiv.innerHTML = '<p>Searching for deals...</p>';

    try {
        // Send the request to Flask backend API route
        const response = await fetch('/api/search', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                zip_code: zipCode
            })
        });

        if (!response.ok) {
            throw new Error(`Server error: ${response.status}`);
        }
        
        const data = await response.json();

        // Pass the data to our display function 
        renderResults(data);

    } catch (error) {
        console.error("Error fetching deals: ", error);
        resultsDiv.innerHTML = '<p style="color: red;">Failed to load deals. Please try again.</p>';
    }
}

function renderResults(data) {
    const resultsDiv = document.getElementById('results');
    resultsDiv.innerHTML = '';

    if (!data.deals || data.deals.length === 0) {
        resultsDiv.innerHTML = '<p>No BOGO deals found in your area</p>';
        return;
    }

    data.deals.forEach(deal => {
        const dealDiv = document.createElement('div');
        dealDiv.style.borderBottom = '1px solid #eee';
        dealDiv.style.marginBottom = '15px';
        dealDiv.style.paddingBottom = '10px';

        const storeHeader = document.createElement('h3');
        storeHeader.textContent = deal.store;
        storeHeader.style.margin = '0 0 5px 0';


        const itemInfo = document.createElement('p');
        itemInfo.textContent = `${deal.item} - (${deal.offer})`;
        itemInfo.style.margin = '0';

        dealDiv.appendChild(storeHeader);
        dealDiv.appendChild(storeAddress);
        dealDiv.appendChild(itemInfo);
        resultsDiv.appendChild(dealDiv);
    });
}