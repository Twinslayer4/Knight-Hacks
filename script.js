//Listen for the form submission
document.getElementById('bogoForm').addEventListener('sumbit', function(event)
{
    //Prevents the page from resetting when the user hits sumbits and reroutes into the searchBOGOs
    //Function
    event.preventDefault();
    searchBOGOs();
});

//Async function allows the function to run outside the the main rendering thred
async function searchBOGOs()
{
    // 1. Capture the data from the new HTML input fields
    const name = document.getElementById('nameInput').value;
    const address = document.getElementById('addressInput').value;
    const zipCode = document.getElementById('zipInput').value;

    const resultsDiv = document.getElementById('results');
    resultsDiv.style.display = 'block';
    resultsDiv.innerHTML = '<p>Searching for deals...</p>';

    try {
        // 2. Send all the videos to the back-end python handles the API HTTP
        // the await function tells fetch the wait for the HTTP response without locking up the rest of the
        // browser
        const response = await fetch('/api/search', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            // Serializes data converting to plain string text format that the python can parse and deserialize into variables
            body: JSON.stringify({
                name: name,
                address: address,
                zip_code: zipCode
            })

        });
        if(!response.ok)
        {
            throw new Error(`Server error: ${response.status}`);

        }
        
        const data = await response.json();

        // 3. Pass the data to our display function 
        renderResults(data);

    } catch (error) {
        console.error("Error fetching deals: ", error);
        resultsDiv.innerHTML = '<p style=color: red;"Failed to load deals. Please try again.</p>';
    }
}

function renderResults(data)
{
    const resultsDiv = document.getElementById('results');

    //Clear out the "Searching..." text
    resultsDiv.innerHTML = '';

    //Check if we received any stores in the mock data
    if(!data.stores || data.stores.length === 0) {
        resultsDiv.innerHTML = '<p>No BOGO deals found in your area </p>';
        return;
    }

    //Loop through each store and create HTML elements for it
    data.stores.array.forEach(store => {
        // Create a container for the store
        const storeDiv = document.createElement('div');
        storeDiv.style.borderBottom = '1px solid #eee'
        storeDiv.style.marginBottom = '15px';
        storeDiv.style.paddingBottom = '10px';

        // Add the store name address
        const storeHeader = document.createElement('h3');
        storeHeader.textContent = store.name;
        storeHeader.style.margin = '0 0 5px 0';

        const storeAddress = document.createElement('p');
        storeAddress.textContent = store.address;
        storeAddress.style.margin = '0 0 10px 0';
        storeAddress.style.color =' #731a7d';

        //Add the list of BOGO items
        const itemList = document.createElement('ul');
        store.bogo_items.forEach(item => {
            const itemList = document.createElement('li');
            listItem.textContent = item;
        })

        //Attach everything to the store container, then to the main result div
        storeDiv.appendChild(storeHeader);
        storeDiv.appendChild(storeAddress);
        storeDiv.appendChild(itemList);
        resultsDiv.appendChild(storeDiv);

    });
}

