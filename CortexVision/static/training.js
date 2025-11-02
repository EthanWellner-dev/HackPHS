class TrainingProgress {
    constructor() {
        this.socket = null;
        this.imageFrames = [];
        this.currentFrame = 0;
        this.progressBar = document.getElementById('training-progress');
        this.statusText = document.getElementById('status-text');
        this.trainingCanvas = document.getElementById('training-animation');
        this.ctx = this.trainingCanvas.getContext('2d');
        this.frameInterval = null;
    }

    connect() {
        this.socket = new WebSocket(`ws://${window.location.host}/ws/training`);
        this.socket.onmessage = (event) => this.handleMessage(event);
        this.socket.onclose = () => {
            console.log('WebSocket connection closed');
            this.stopAnimation();
        };
    }

    handleMessage(event) {
        const data = JSON.parse(event.data);
        
        if (data.type === 'progress') {
            this.updateProgress(data);
        } else if (data.type === 'images') {
            this.loadTrainingImages(data.images);
        } else if (data.type === 'error') {
            this.showError(data.message);
        } else if (data.type === 'complete') {
            this.handleCompletion();
        }
    }

    updateProgress(data) {
        const { processed, total, stage } = data;
        const percent = (processed / total) * 100;
        this.progressBar.style.width = `${percent}%`;
        this.statusText.textContent = `${stage}: ${processed}/${total} images`;
    }

    async loadTrainingImages(imageUrls) {
        this.imageFrames = [];
        for (const url of imageUrls) {
            try {
                const img = new Image();
                img.src = url;
                await new Promise((resolve, reject) => {
                    img.onload = resolve;
                    img.onerror = reject;
                });
                this.imageFrames.push(img);
            } catch (error) {
                console.error('Failed to load image:', error);
            }
        }
        this.startAnimation();
    }

    startAnimation() {
        if (this.frameInterval) return;
        
        this.frameInterval = setInterval(() => {
            if (this.imageFrames.length === 0) return;
            
            const img = this.imageFrames[this.currentFrame];
            
            // Clear canvas and draw image
            this.ctx.clearRect(0, 0, this.trainingCanvas.width, this.trainingCanvas.height);
            
            // Calculate dimensions to maintain aspect ratio
            const scale = Math.min(
                this.trainingCanvas.width / img.width,
                this.trainingCanvas.height / img.height
            );
            
            const width = img.width * scale;
            const height = img.height * scale;
            const x = (this.trainingCanvas.width - width) / 2;
            const y = (this.trainingCanvas.height - height) / 2;
            
            this.ctx.drawImage(img, x, y, width, height);
            
            this.currentFrame = (this.currentFrame + 1) % this.imageFrames.length;
        }, 200); // Change frame every 200ms
    }

    stopAnimation() {
        if (this.frameInterval) {
            clearInterval(this.frameInterval);
            this.frameInterval = null;
        }
    }

    showError(message) {
        this.statusText.textContent = `Error: ${message}`;
        this.statusText.classList.add('error');
    }

    handleCompletion() {
        this.statusText.textContent = 'Training complete!';
        this.progressBar.style.width = '100%';
        this.progressBar.classList.add('complete');
        setTimeout(() => {
            window.location.href = '/';
        }, 3000);
    }
}

document.addEventListener('DOMContentLoaded', () => {
    const training = new TrainingProgress();
    training.connect();
});