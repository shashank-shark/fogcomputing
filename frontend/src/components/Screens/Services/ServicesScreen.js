import React from "react";

import CardInterative from "../../Cards/CardInteractive";


const ServicesScreen = () => {

    return (
        <div style={styles.cardContainerStyle}>
            <CardInterative heading="Messaging" description="There can be only one winner, Let's Go" />
            <CardInterative heading="Messaging" description="There can be only one winner, Let's Go" />
            <CardInterative heading="Messaging" description="There can be only one winner, Let's Go" />
            <CardInterative heading="Messaging" description="There can be only one winner, Let's Go" />
            <CardInterative heading="Messaging" description="There can be only one winner, Let's Go" />
            <CardInterative heading="Messaging" description="There can be only one winner, Let's Go" />
            <CardInterative heading="Messaging" description="There can be only one winner, Let's Go" />
        </div>
    );
};

const styles = {
    cardContainerStyle : {
        display: 'grid',
        gridTemplateColumns: '1fr 1fr 1fr',
        gridGap: '2%',
        marginTop: '1%'
    }
}

export default ServicesScreen;